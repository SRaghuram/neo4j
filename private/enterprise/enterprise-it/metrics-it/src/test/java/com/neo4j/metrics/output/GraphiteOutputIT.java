/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.neo4j.configuration.helpers.GlobbingPattern;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.MetricsSettings.graphite_enabled;
import static com.neo4j.configuration.MetricsSettings.graphite_server;
import static com.neo4j.configuration.MetricsSettings.metrics_enabled;
import static com.neo4j.configuration.MetricsSettings.metrics_filter;
import static com.neo4j.configuration.MetricsSettings.metrics_prefix;
import static io.netty.handler.codec.Delimiters.lineDelimiter;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.test.assertion.Assert.assertEventually;

@TestDirectoryExtension
class GraphiteOutputIT
{
    private static final String CUSTOM_METRICS_PREFIX = "hello";

    @Inject
    private TestDirectory testDirectory;

    private Channel serverChannel;
    private NioEventLoopGroup eventLoopGroup;
    private DatabaseManagementService managementService;

    private final Queue<String> receivedMetrics = new LinkedBlockingQueue<>( 1000 );

    @BeforeEach
    void setUp() throws Exception
    {
        eventLoopGroup = new NioEventLoopGroup( 2 );
        serverChannel = startFakeGraphiteServer( eventLoopGroup, receivedMetrics );
        managementService = startDatabaseWithGraphiteMetrics( testDirectory.homePath(), serverChannel.localAddress() );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        executeAll(
                managementService::shutdown,
                () -> serverChannel.close().sync(),
                () -> eventLoopGroup.shutdownGracefully( 1, 1, SECONDS ).sync() );
    }

    @Test
    void shouldReportMetricsToGraphiteWithoutDuplicatedPrefix() throws Exception
    {
        assertEventually( "A fake Graphite server did not receive any metrics", receivedMetrics::size, value -> value >= 100, 2, MINUTES );

        var doublePrefix = CUSTOM_METRICS_PREFIX + "." + CUSTOM_METRICS_PREFIX;
        for ( var metric : receivedMetrics )
        {
            assertThat( metric ).doesNotStartWith( doublePrefix );
        }
    }

    private static DatabaseManagementService startDatabaseWithGraphiteMetrics( Path homeDir, SocketAddress graphiteServerAddress )
    {
        var address = (InetSocketAddress) graphiteServerAddress;

        return new TestEnterpriseDatabaseManagementServiceBuilder( homeDir )
                .setConfig( metrics_enabled, true )
                .setConfig( metrics_filter, GlobbingPattern.create( "*" ) )
                .setConfig( metrics_prefix, CUSTOM_METRICS_PREFIX )
                .setConfig( graphite_enabled, true )
                .setConfig( graphite_server, new org.neo4j.configuration.helpers.SocketAddress( address.getHostString(), address.getPort() ) )
                .build();
    }

    private static Channel startFakeGraphiteServer( NioEventLoopGroup eventLoopGroup, Queue<String> metrics ) throws InterruptedException
    {
        return new ServerBootstrap()
                .channel( NioServerSocketChannel.class )
                .group( eventLoopGroup )
                .childHandler( new FakeGraphiteServerInitializer( metrics ) )
                .bind( 0 )
                .sync()
                .channel();
    }

    private static class FakeGraphiteServerInitializer extends ChannelInitializer<SocketChannel>
    {
        final Queue<String> metrics;

        FakeGraphiteServerInitializer( Queue<String> metrics )
        {
            this.metrics = metrics;
        }

        @Override
        protected void initChannel( SocketChannel ch )
        {
            ch.pipeline().addLast( new MetricsDecoder() );
            ch.pipeline().addLast( new MetricsStore( metrics ) );
        }
    }

    private static class MetricsDecoder extends DelimiterBasedFrameDecoder
    {
        MetricsDecoder()
        {
            super( toIntExact( mebiBytes( 10 ) ), lineDelimiter() );
        }
    }

    private static class MetricsStore extends SimpleChannelInboundHandler<ByteBuf>
    {
        final Queue<String> metrics;

        MetricsStore( Queue<String> metrics )
        {
            this.metrics = metrics;
        }

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg )
        {
            metrics.offer( msg.toString( UTF_8 ) );
        }
    }
}
