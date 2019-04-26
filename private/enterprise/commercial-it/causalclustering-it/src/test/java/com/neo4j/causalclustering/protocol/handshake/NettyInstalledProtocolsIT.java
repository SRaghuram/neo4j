/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
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
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.ports.PortAuthority;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_2;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.COMPRESSION;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
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
                .collect( Collectors.toList() );
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private static Parameters raft2WithCompressionModifiers( Optional<ModifierProtocol> protocol )
    {
        List<String> versions = protocol.stream().map( Protocol::implementation ).collect( Collectors.toList() );
        return new Parameters( "Raft 2, modifiers: " + protocol, new ApplicationSupportedProtocols( RAFT, singletonList( RAFT_2.implementation() ) ),
                singletonList( new ModifierSupportedProtocols( COMPRESSION, versions ) ) );
    }

    @ParameterizedTest( name = "{0}" )
    @MethodSource( "data" )
    void shouldSuccessfullySendAndReceiveAMessage( Parameters parameters ) throws Throwable
    {
        startServerAndConnect( parameters );

        // given
        RaftMessages.Heartbeat raftMessage = new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 1, 2, 3 );
        RaftMessages.RaftIdAwareMessage<RaftMessages.Heartbeat> networkMessage =
                RaftMessages.RaftIdAwareMessage.of( new RaftId( UUID.randomUUID() ), raftMessage );

        // when
        client.send( networkMessage ).syncUninterruptibly();

        // then
        assertEventually(
                messages -> String.format( "Received messages %s should contain message decorating %s", messages, raftMessage ),
                () -> server.received(),
                contains( messageMatches( networkMessage ) ), TIMEOUT_SECONDS, SECONDS );
    }

    private Server server;
    private Client client;

    private void startServerAndConnect( Parameters parameters )
    {
        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( ApplicationProtocols.values(), parameters.applicationSupportedProtocol );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( ModifierProtocols.values(), parameters.modifierSupportedProtocols );

        NettyPipelineBuilderFactory serverPipelineBuilderFactory = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );
        NettyPipelineBuilderFactory clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );

        server = new Server( serverPipelineBuilderFactory );
        server.start( applicationProtocolRepository, modifierProtocolRepository, logProvider );

        Config config = Config.builder().withSetting( CausalClusteringSettings.handshake_timeout, TIMEOUT_SECONDS + "s" ).build();

        client = new Client( applicationProtocolRepository, modifierProtocolRepository, clientPipelineBuilderFactory, config, logProvider );

        client.connect( server.port() );
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
        private NettyPipelineBuilderFactory pipelineBuilderFactory;

        ChannelInboundHandler nettyHandler = new SimpleChannelInboundHandler<Object>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, Object msg )
            {
                received.add( msg );
            }
        };

        Server( NettyPipelineBuilderFactory pipelineBuilderFactory )
        {
            this.pipelineBuilderFactory = pipelineBuilderFactory;
        }

        void start( final ApplicationProtocolRepository applicationProtocolRepository, final ModifierProtocolRepository modifierProtocolRepository,
                LogProvider logProvider )
        {
            RaftProtocolServerInstallerV2.Factory raftFactoryV2 =
                    new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineBuilderFactory, logProvider );
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                    new ProtocolInstallerRepository<>( List.of( raftFactoryV2 ), ModifierProtocolInstaller.allServerInstallers );

            eventLoopGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap().group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_REUSEADDR, true )
                    .localAddress( PortAuthority.allocatePort() )
                    .childHandler( new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                            protocolInstallerRepository, pipelineBuilderFactory, logProvider ).asChannelInitializer() );

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
        private Bootstrap bootstrap;
        private NioEventLoopGroup eventLoopGroup;
        private Channel channel;
        private HandshakeClientInitializer handshakeClientInitializer;

        Client( ApplicationProtocolRepository applicationProtocolRepository, ModifierProtocolRepository modifierProtocolRepository,
                NettyPipelineBuilderFactory pipelineBuilderFactory, Config config, LogProvider logProvider )
        {
            RaftProtocolClientInstallerV2.Factory raftFactoryV2 = new RaftProtocolClientInstallerV2.Factory( pipelineBuilderFactory, logProvider );
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository =
                    new ProtocolInstallerRepository<>( List.of( raftFactoryV2 ), ModifierProtocolInstaller.allClientInstallers );
            eventLoopGroup = new NioEventLoopGroup();
            Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
            handshakeClientInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                    protocolInstallerRepository, pipelineBuilderFactory, handshakeTimeout, logProvider, logProvider );
            bootstrap = new Bootstrap().group( eventLoopGroup ).channel( NioSocketChannel.class ).handler( handshakeClientInitializer );
        }

        @SuppressWarnings( "SameParameterValue" )
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
    }

    private Matcher<Object> messageMatches( RaftMessages.RaftIdAwareMessage<? extends RaftMessages.RaftMessage> expected )
    {
        return new MessageMatcher( expected );
    }

    class MessageMatcher extends BaseMatcher<Object>
    {
        private final RaftMessages.RaftIdAwareMessage<? extends RaftMessages.RaftMessage> expected;

        MessageMatcher( RaftMessages.RaftIdAwareMessage<? extends RaftMessages.RaftMessage> expected )
        {
            this.expected = expected;
        }

        @Override
        public boolean matches( Object item )
        {
            if ( item instanceof RaftMessages.RaftIdAwareMessage<?> )
            {
                RaftMessages.RaftIdAwareMessage<?> message = (RaftMessages.RaftIdAwareMessage<?>) item;
                return message.raftId().equals( expected.raftId() ) && message.message().equals( expected.message() );
            }
            else
            {
                return false;
            }
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Raft ID " ).appendValue( expected.raftId() ).appendText( " message " ).appendValue( expected.message() );
        }
    }
}
