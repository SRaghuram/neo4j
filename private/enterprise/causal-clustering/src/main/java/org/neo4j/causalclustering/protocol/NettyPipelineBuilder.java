/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.neo4j.causalclustering.messaging.MessageGate;
import org.neo4j.function.ThrowingAction;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.neo4j.util.FeatureToggles.flag;

/**
 * Builder and installer of pipelines.
 * <p>
 * Makes sures to install sane last-resort error handling and
 * handles the construction of common patterns, like framing.
 * <p>
 * Do not modify the names of handlers you install.
 */
public abstract class NettyPipelineBuilder<O extends ProtocolInstaller.Orientation, BUILDER extends NettyPipelineBuilder<O, BUILDER>>
{
    private static final boolean DEBUG = flag( NettyPipelineBuilder.class, "DEBUG", false );

    static final String MESSAGE_GATE_NAME = "message_gate";
    static final String ERROR_HANDLER_TAIL = "error_handler_tail";
    static final String ERROR_HANDLER_HEAD = "error_handler_head";

    private final ChannelPipeline pipeline;
    private final Log log;
    private final List<HandlerInfo> handlerInfos = new ArrayList<>();

    private Predicate<Object> gatePredicate;
    private Runnable closeHandler;

    @SuppressWarnings( "unchecked" )
    private BUILDER self = (BUILDER) this;

    NettyPipelineBuilder( ChannelPipeline pipeline, Log log )
    {
        this.pipeline = pipeline;
        this.log = log;
    }

    /**
     * Entry point for the client builder.
     *
     * @param pipeline The pipeline to build for.
     * @param log The log used for last-resort errors occurring in the pipeline.
     * @return The client builder.
     */
    public static ClientNettyPipelineBuilder client( ChannelPipeline pipeline, Log log )
    {
        return new ClientNettyPipelineBuilder( pipeline, log );
    }

    /**
     * Entry point for the server builder.
     *
     * @param pipeline The pipeline to build for.
     * @param log The log used for last-resort errors occurring in the pipeline.
     * @return The server builder.
     */
    public static ServerNettyPipelineBuilder server( ChannelPipeline pipeline, Log log )
    {
        return new ServerNettyPipelineBuilder( pipeline, log );
    }

    /**
     * Adds buffer framing to the pipeline. Useful for pipelines marshalling
     * complete POJOs as an example using {@link MessageToByteEncoder} and
     * {@link ByteToMessageDecoder}.
     */
    public abstract BUILDER addFraming();

    public BUILDER modify( ModifierProtocolInstaller<O> modifier )
    {
        modifier.apply( this );
        return self;
    }

    public BUILDER modify( List<ModifierProtocolInstaller<O>> modifiers )
    {
        modifiers.forEach( this::modify );
        return self;
    }

    /**
     * Adds handlers to the pipeline.
     * <p>
     * The pipeline builder controls the internal names of the handlers in the
     * pipeline and external actors are forbidden from manipulating them.
     *
     * @param name The name of the handler, which must be unique.
     * @param newHandlers The new handlers.
     * @return The builder.
     */
    public BUILDER add( String name, List<ChannelHandler> newHandlers )
    {
        newHandlers.stream().map( handler -> new HandlerInfo( name, handler ) ).forEachOrdered( handlerInfos::add );
        return self;
    }

    /**
     * @see #add(String, List)
     */
    public BUILDER add( String name, ChannelHandler... newHandlers )
    {
        return add( name, asList( newHandlers ) );
    }

    public BUILDER addGate( Predicate<Object> gatePredicate )
    {
        if ( this.gatePredicate != null )
        {
            throw new IllegalStateException( "Cannot have more than one gate." );
        }
        this.gatePredicate = gatePredicate;
        return self;
    }

    public BUILDER onClose( Runnable closeHandler )
    {
        if ( this.closeHandler != null )
        {
            throw new IllegalStateException( "Cannot have more than one close handler." );
        }
        this.closeHandler = closeHandler;
        return self;
    }

    /**
     * Installs the built pipeline and removes any old pipeline.
     */
    public void install()
    {
        ensureErrorHandling();
        installGate();
        clearUserHandlers();

        String userHead = ERROR_HANDLER_HEAD;
        for ( HandlerInfo info : handlerInfos )
        {
            pipeline.addAfter( userHead, info.name, info.handler );
            userHead = info.name;
        }
    }

    private void installGate()
    {
        if ( pipeline.get( MESSAGE_GATE_NAME ) != null && gatePredicate != null )
        {
            throw new IllegalStateException( "Cannot have more than one gate." );
        }
        else if ( gatePredicate != null )
        {
            pipeline.addBefore( ERROR_HANDLER_TAIL, MESSAGE_GATE_NAME, new MessageGate( gatePredicate ) );
        }
    }

    private void clearUserHandlers()
    {
        pipeline.names().stream()
                .filter( this::isNotDefault )
                .filter( this::isNotErrorHandler )
                .filter( this::isNotGate )
                .forEach( pipeline::remove );
    }

    private boolean isNotDefault( String name )
    {
        // these are netty internal handlers for head and tail
        return pipeline.get( name ) != null;
    }

    private boolean isNotErrorHandler( String name )
    {
        return !name.equals( ERROR_HANDLER_HEAD ) && !name.equals( ERROR_HANDLER_TAIL );
    }

    private boolean isNotGate( String name )
    {
        return !name.equals( MESSAGE_GATE_NAME );
    }

    private void ensureErrorHandling()
    {
        int size = pipeline.names().size();

        if ( pipeline.names().get( 0 ).equals( ERROR_HANDLER_HEAD ) )
        {
            if ( !pipeline.names().get( size - 2 ).equals( ERROR_HANDLER_TAIL ) ) // last position before netty's tail sentinel
            {
                throw new IllegalStateException( "Both error handlers must exist." );
            }
            return;
        }

        // inbound goes in the direction from first->last
        pipeline.addLast( ERROR_HANDLER_TAIL, new ChannelDuplexHandler()
        {
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
            {
                swallow( () -> log.error( format( "Exception in inbound for channel: %s", ctx.channel() ), cause ) );
                ReferenceCountUtil.release( cause );
                swallow( ctx::close );
            }

            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                log.error( "Unhandled inbound message: %s for channel: %s", msg, ctx.channel() );
                ReferenceCountUtil.release( msg );
                ctx.close();
            }

            // this is the first handler for an outbound message, and attaches a listener to its promise if possible
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                if ( DEBUG )
                {
                    log.info( "OUTBOUND: " + msg );
                }

                // if the promise is a void-promise, then exceptions will instead propagate to the
                // exceptionCaught handler on the outbound handler further below
                if ( !promise.isVoid() )
                {
                    promise.addListener( (ChannelFutureListener) future -> {
                        if ( !future.isSuccess() )
                        {
                            swallow( () -> log.error( format( "Exception in outbound for channel: %s", future.channel() ), future.cause() ) );
                            swallow( ctx::close );
                        }
                    } );
                }
                ctx.write( msg, promise );
            }

            @Override
            public void channelInactive( ChannelHandlerContext ctx )
            {
                if ( closeHandler != null )
                {
                    closeHandler.run();
                }
                ctx.fireChannelInactive();
            }
        } );

        pipeline.addFirst( ERROR_HANDLER_HEAD, new ChannelDuplexHandler()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                logByteBuf( ctx, "INBOUND", msg );
                ctx.fireChannelRead( msg );
            }

            // exceptions which did not get fulfilled on the promise of a write, etc.
            @Override
            public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
            {
                swallow( () -> log.error( format( "Exception in outbound for channel: %s", ctx.channel() ), cause ) );
                ReferenceCountUtil.release( cause );
                swallow( ctx::close );
            }

            // netty can only handle bytes in the form of ByteBuf, so if you reach this then you are
            // perhaps trying to send a POJO without having a suitable encoder
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                if ( !(msg instanceof ByteBuf) )
                {
                    log.error( "Unhandled outbound message: %s for channel: %s", msg, ctx.channel() );
                    ReferenceCountUtil.release( msg );
                    ctx.close();
                }
                else
                {
                    logByteBuf( ctx, "OUTBOUND", msg );
                    ctx.write( msg, promise );
                }
            }
        } );
    }

    /**
     * An throwable-swallowing execution of an action. Used in last-resort exception handlers.
     */
    private void swallow( ThrowingAction<Exception> action )
    {
        try
        {
            action.apply();
        }
        catch ( Throwable ignored )
        {

        }
    }

    private void logByteBuf( ChannelHandlerContext ctx, String prefix, Object msg )
    {
        if ( DEBUG )
        {
            log.info( prefix + ": " + ctx.channel() +
                    "\n" + msg.toString() +
                    "\n" + ByteBufUtil.prettyHexDump( (ByteBuf) msg ) );
        }
    }

    private static class HandlerInfo
    {
        private final String name;
        private final ChannelHandler handler;

        HandlerInfo( String name, ChannelHandler handler )
        {
            this.name = name;
            this.handler = handler;
        }
    }
}
