/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftIdFactory;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.init.ClientChannelInitializer;
import com.neo4j.causalclustering.protocol.init.ServerChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.assertj.core.api.HamcrestCondition;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.ports.PortAuthority;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.handshake_timeout;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.inbound_connection_initialization_logging_enabled;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_2_0;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.COMPRESSION;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;

class NettyInstalledProtocolsIT
{
    private static final int TIMEOUT_SECONDS = 10;

    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );

    private static Collection<Parameters> data()
    {
        Stream<Optional<ModifierProtocol>> noModifierProtocols = Stream.of( Optional.empty() );
        Stream<Optional<ModifierProtocol>> individualModifierProtocols = Stream.of( ModifierProtocols.values() ).map( Optional::of );

        return Stream
                .concat( noModifierProtocols, individualModifierProtocols )
                .flatMap( protocol -> Stream.of( raft2WithCompressionModifiers( protocol ) ) )
                .collect( toList() );
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private static Parameters raft2WithCompressionModifiers( Optional<ModifierProtocol> protocol )
    {
        List<String> versions = protocol.stream().map( Protocol::implementation ).collect( toList() );
        return new Parameters( "Raft 2, modifiers: " + protocol, new ApplicationSupportedProtocols( RAFT, singletonList( RAFT_2_0.implementation() ) ),
                singletonList( new ModifierSupportedProtocols( COMPRESSION, versions ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "data" )
    void shouldSuccessfullySendAndReceiveAMessage( Parameters parameters ) throws Throwable
    {
        startServerAndConnect( parameters );

        // given
        RaftMessages.Heartbeat raftMessage = new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 1, 2, 3 );
        RaftMessages.DistributedRaftMessage<RaftMessages.Heartbeat> networkMessage =
                RaftMessages.DistributedRaftMessage.of( RaftIdFactory.random(), raftMessage );

        // when
        client.send( networkMessage ).syncUninterruptibly();

        // then
        assertEventually(
                messages -> String.format( "Received messages %s should contain message decorating %s", messages, raftMessage ),
                () -> server.received(),
                new HamcrestCondition<>( contains( messageMatches( networkMessage ) ) ), TIMEOUT_SECONDS, SECONDS );
    }

    @Test
    void shouldLogInboundConnectionsByDefault()
    {
        startServerAndConnect( raft2WithCompressionModifiers( Optional.empty() ) );

        assertThat( logProvider ).forClass( ServerChannelInitializer.class ).forLevel( AssertableLogProvider.Level.INFO ).
                containsMessages( "Initializing server channel" );

        assertThat( logProvider ).forClass( HandshakeServerInitializer.class ).forLevel( AssertableLogProvider.Level.INFO ).
                containsMessages( "Installing handshake server" );
    }

    @Test
    void shouldNotLogInboundConnectionsWhenLoggingTurnedOff()
    {
        var configBuilder = Config.newBuilder().set( inbound_connection_initialization_logging_enabled, false );

        startServerAndConnect( raft2WithCompressionModifiers( Optional.empty() ), configBuilder );

        assertThat( logProvider ).forClass( ServerChannelInitializer.class ).forLevel( AssertableLogProvider.Level.INFO ).
                doesNotContainMessage( "Initializing server channel" );

        assertThat( logProvider ).forClass( HandshakeServerInitializer.class ).forLevel( AssertableLogProvider.Level.INFO ).
                doesNotContainMessage( "Installing handshake server" );
    }

    private Server server;
    private Client client;

    private void startServerAndConnect( Parameters parameters )
    {
        startServerAndConnect( parameters, Config.newBuilder() );
    }

    private void startServerAndConnect( Parameters parameters, Config.Builder configBuilder )
    {
        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( ApplicationProtocols.values(), parameters.applicationSupportedProtocol );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( ModifierProtocols.values(), parameters.modifierSupportedProtocols );

        NettyPipelineBuilderFactory serverPipelineBuilderFactory = NettyPipelineBuilderFactory.insecure();
        NettyPipelineBuilderFactory clientPipelineBuilderFactory = NettyPipelineBuilderFactory.insecure();

        Config config = configBuilder.set( handshake_timeout, Duration.ofSeconds( TIMEOUT_SECONDS ) ).build();

        server = new Server( serverPipelineBuilderFactory, config );
        server.start( applicationProtocolRepository, modifierProtocolRepository, logProvider );

        client = new Client( applicationProtocolRepository, modifierProtocolRepository, clientPipelineBuilderFactory, config, logProvider );

        client.connect( server.port() );
        client.verifyProtocolStack( parameters );
    }

    @AfterEach
    void afterEach()
    {
        client.disconnect();
        server.stop();
        logProvider.clear();
    }

    private static class Parameters
    {
        final String name;
        final ApplicationSupportedProtocols applicationSupportedProtocol;
        final Collection<ModifierSupportedProtocols> modifierSupportedProtocols;

        Parameters( String name, ApplicationSupportedProtocols applicationSupportedProtocol,
                Collection<ModifierSupportedProtocols> modifierSupportedProtocols )
        {
            this.name = name;
            this.applicationSupportedProtocol = applicationSupportedProtocol;
            this.modifierSupportedProtocols = modifierSupportedProtocols;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    static class Server
    {
        private Channel channel;
        private NioEventLoopGroup eventLoopGroup;
        private final List<Object> received = new CopyOnWriteArrayList<>();
        private final NettyPipelineBuilderFactory pipelineBuilderFactory;
        private final Config config;

        ChannelInboundHandler nettyHandler = new SimpleChannelInboundHandler<>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, Object msg )
            {
                received.add( msg );
            }
        };

        Server( NettyPipelineBuilderFactory pipelineBuilderFactory, Config config )
        {
            this.pipelineBuilderFactory = pipelineBuilderFactory;
            this.config = config;
        }

        void start( final ApplicationProtocolRepository applicationProtocolRepository, final ModifierProtocolRepository modifierProtocolRepository,
                LogProvider logProvider )
        {
            RaftProtocolServerInstallerV2.Factory raftFactoryV2 =
                    new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineBuilderFactory, logProvider );
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                    new ProtocolInstallerRepository<>( List.of( raftFactoryV2 ), ModifierProtocolInstaller.allServerInstallers );

            eventLoopGroup = new NioEventLoopGroup();

            HandshakeServerInitializer handshakeInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                    protocolInstallerRepository, pipelineBuilderFactory, logProvider, config );

            ServerChannelInitializer channelInitializer = new ServerChannelInitializer( handshakeInitializer, pipelineBuilderFactory,
                    config.get( handshake_timeout ), logProvider, config );

            ServerBootstrap bootstrap = new ServerBootstrap().group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_REUSEADDR, true )
                    .localAddress( PortAuthority.allocatePort() )
                    .childHandler( channelInitializer.asChannelInitializer() );

            channel = bootstrap.bind().syncUninterruptibly().channel();
        }

        void stop()
        {
            channel.close().syncUninterruptibly();
            eventLoopGroup.shutdownGracefully( 0, TIMEOUT_SECONDS, SECONDS );
        }

        int port()
        {
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }

        Collection<Object> received()
        {
            return received;
        }
    }

    static class Client
    {
        private final Bootstrap bootstrap;
        private final NioEventLoopGroup eventLoopGroup;
        private Channel channel;
        private final HandshakeClientInitializer handshakeInitializer;

        Client( ApplicationProtocolRepository applicationProtocolRepository, ModifierProtocolRepository modifierProtocolRepository,
                NettyPipelineBuilderFactory pipelineBuilderFactory, Config config, LogProvider logProvider )
        {
            RaftProtocolClientInstallerV2.Factory raftFactoryV2 = new RaftProtocolClientInstallerV2.Factory( pipelineBuilderFactory, logProvider );
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository =
                    new ProtocolInstallerRepository<>( List.of( raftFactoryV2 ), ModifierProtocolInstaller.allClientInstallers );
            eventLoopGroup = new NioEventLoopGroup();
            Duration handshakeTimeout = config.get( handshake_timeout );
            handshakeInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                    protocolInstallerRepository, pipelineBuilderFactory, handshakeTimeout, logProvider, logProvider );
            ClientChannelInitializer channelInitializer = new ClientChannelInitializer( handshakeInitializer, pipelineBuilderFactory,
                    handshakeTimeout, logProvider );
            bootstrap = new Bootstrap().group( eventLoopGroup ).channel( NioSocketChannel.class ).handler( channelInitializer );
        }

        void connect( int port )
        {
            ChannelFuture channelFuture = bootstrap.connect( "localhost", port ).syncUninterruptibly();
            channel = channelFuture.channel();
        }

        void disconnect()
        {
            if ( channel != null )
            {
                channel.close().syncUninterruptibly();
                eventLoopGroup.shutdownGracefully( 0, TIMEOUT_SECONDS, SECONDS ).syncUninterruptibly();
            }
        }

        ChannelFuture send( Object message )
        {
            return channel.writeAndFlush( message );
        }

        void verifyProtocolStack( Parameters parameters )
        {
            var protocolStackFuture = channel.attr( ChannelAttribute.PROTOCOL_STACK ).get();
            assertNotNull( protocolStackFuture );
            var protocolStack = protocolStackFuture.join();

            var applicationProtocol = protocolStack.applicationProtocol();
            var modifierProtocols = protocolStack.modifierProtocols();

            assertEquals( parameters.applicationSupportedProtocol.identifier().canonicalName(), applicationProtocol.category() );

            var expectedModifierProtocolCategories = parameters.modifierSupportedProtocols.stream()
                    .map( mp -> mp.identifier().canonicalName() )
                    .collect( toSet() );

            var actualModifierProtocolCategories = modifierProtocols.stream()
                    .map( Protocol::category )
                    .collect( toSet() );

            assertEquals( expectedModifierProtocolCategories, actualModifierProtocolCategories );
        }
    }

    private Matcher<Object> messageMatches( RaftMessages.DistributedRaftMessage<? extends RaftMessages.RaftMessage> expected )
    {
        return new MessageMatcher( expected );
    }

    class MessageMatcher extends BaseMatcher<Object>
    {
        private final RaftMessages.DistributedRaftMessage<? extends RaftMessages.RaftMessage> expected;

        MessageMatcher( RaftMessages.DistributedRaftMessage<? extends RaftMessages.RaftMessage> expected )
        {
            this.expected = expected;
        }

        @Override
        public boolean matches( Object item )
        {
            if ( item instanceof RaftMessages.DistributedRaftMessage<?> )
            {
                RaftMessages.DistributedRaftMessage<?> message = (RaftMessages.DistributedRaftMessage<?>) item;
                return message.raftId().equals( expected.raftId() ) && message.message().equals( expected.message() );
            }
            return false;
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Raft ID " ).appendValue( expected.raftId() ).appendText( " message " ).appendValue( expected.message() );
        }
    }
}
