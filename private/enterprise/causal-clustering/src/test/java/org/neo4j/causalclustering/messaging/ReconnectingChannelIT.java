/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ClientNettyPipelineBuilder;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilder;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class ReconnectingChannelIT
{
    private static final int PORT = PortAuthority.allocatePort();
    private static final long DEFAULT_TIMEOUT_MS = 20_000;
    private final Log log = NullLogProvider.getInstance().getLog( getClass() );
    private final ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", PORT );
    private final Server server = new Server( channel -> {}, listenAddress, "test-server" );
    private EventLoopGroup elg;
    private ReconnectingChannel channel;
    private AtomicInteger childCount = new AtomicInteger();
    private final ChannelHandler childCounter = new ChannelInitializer<SocketChannel>()
    {
        @Override
        protected void initChannel( SocketChannel ch )
        {
            ch.pipeline().addLast( new ChannelInboundHandlerAdapter()
            {
                @Override
                public void channelActive( ChannelHandlerContext ctx )
                {
                    childCount.incrementAndGet();
                }

                @Override
                public void channelInactive( ChannelHandlerContext ctx )
                {
                    childCount.decrementAndGet();
                }
            } );
        }
    };

    @Before
    public void before()
    {
        elg = new NioEventLoopGroup( 0 );
        Bootstrap bootstrap = new Bootstrap().channel( NioSocketChannel.class ).group( elg ).handler( childCounter );
        channel = new ReconnectingChannel( bootstrap, elg.next(), listenAddress, Duration.ofSeconds( 5 ), log );
    }

    @After
    public void after() throws Throwable
    {
        elg.shutdownGracefully( 0, DEFAULT_TIMEOUT_MS, MILLISECONDS ).awaitUninterruptibly();
        server.stop();
    }

    @Test
    public void shouldLogErrorsProperly() throws Exception
    {
        // given
        EmbeddedChannel channel = new EmbeddedChannel();
        Bootstrap bootstrap = mock( Bootstrap.class );
        when( bootstrap.connect( any() ) ).thenReturn( channel.newSucceededFuture() );

        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( getClass() );
        ReconnectingChannel failingChannel = new ReconnectingChannel( bootstrap, elg.next(), listenAddress, Duration.ofSeconds( 5 ), log );

        ClientNettyPipelineBuilder pipelineBuilder = NettyPipelineBuilder.client( channel.pipeline(), log );

        String message = "This is a bad handler!";
        RuntimeException ex = new RuntimeException( message );
        ChannelHandler badHandler = new ChannelOutboundHandlerAdapter()
        {
            @Override
            public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise )
            {
                throw ex;
            }

        };

        pipelineBuilder
                .add( "bad_handler", badHandler )
                .install();

        // when
        failingChannel.writeFlushAndForget( "Hi" );

        // then
        ThrowingSupplier<Boolean,Exception> test = () -> logProvider.containsMatchingLogCall( inLog( getClass() )
                .error( startsWith( "Exception in outbound" ), equalTo( ex ) ) );
        assertEventually( ignore -> String.format( "Logs did not contain expected message. Logs: %n %s", logProvider.serialize() ),
                test, is( true  ), 10, SECONDS );
    }

    @Test
    public void shouldBeAbleToSendMessage() throws Throwable
    {
        // given
        server.start();

        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will be successfully completed
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldAllowDeferredSend() throws Throwable
    {
        // given
        server.start();

        // this is slightly racy, but generally we will send before the channel was connected
        // this is benign in the sense that the test will pass in the condition where it was already connected as well

        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will be successfully completed
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test( expected = ExecutionException.class )
    public void shouldFailSendWhenNoServer() throws Exception
    {
        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will throw
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldReconnectAfterServerComesBack() throws Throwable
    {
        // given
        server.start();

        // when
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );

        // then will not throw
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );

        // when
        server.stop();
        fSend = channel.writeAndFlush( emptyBuffer() );

        // then will throw
        try
        {
            fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
            fail( "Expected failure to send" );
        }
        catch ( ExecutionException ex )
        {
            // pass
        }

        // when
        server.start();
        fSend = channel.writeAndFlush( emptyBuffer() );

        // then will not throw
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    @Test
    public void shouldNotAllowSendingOnDisposedChannel() throws Throwable
    {
        // given
        server.start();

        // ensure we are connected
        Future<Void> fSend = channel.writeAndFlush( emptyBuffer() );
        fSend.get( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        assertEventually( childCount::get, equalTo( 1 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );

        // when
        channel.dispose();

        try
        {
            channel.writeAndFlush( emptyBuffer() );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }

        // then
        assertEventually( childCount::get, equalTo( 0 ), DEFAULT_TIMEOUT_MS, MILLISECONDS );
    }

    private ByteBuf emptyBuffer()
    {
        return ByteBufAllocator.DEFAULT.buffer();
    }
}
