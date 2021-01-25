/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.protocol.handshake.GateEvent;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Gates messages and keeps them on a queue until the gate is either
 * opened successfully or closed forever.
 */
@ChannelHandler.Sharable
public class MessageGate extends ChannelDuplexHandler
{
    private final Predicate<Object> gated;

    private List<GatedWrite> pending = new ArrayList<>();

    public MessageGate( Predicate<Object> gated )
    {
        this.gated = gated;
    }

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
    {
        if ( evt instanceof GateEvent )
        {
            if ( GateEvent.getSuccess().equals( evt ) )
            {
                for ( GatedWrite write : pending )
                {
                    ctx.write( write.msg, write.promise );
                }

                ctx.channel().pipeline().remove( this );
            }

            pending.clear();
            pending = null;
        }
        else
        {
            super.userEventTriggered( ctx, evt );
        }
    }

    @Override
    public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
    {
        if ( !gated.test( msg ) )
        {
            ctx.write( msg, promise );
        }
        else if ( pending != null )
        {
            pending.add( new GatedWrite( msg, promise ) );
        }
        else
        {
            promise.setFailure( new RuntimeException( "Gate failed and has been permanently closed." ) );
        }
    }

    static class GatedWrite
    {
        final Object msg;
        final ChannelPromise promise;

        GatedWrite( Object msg, ChannelPromise promise )
        {
            this.msg = msg;
            this.promise = promise;
        }
    }
}
