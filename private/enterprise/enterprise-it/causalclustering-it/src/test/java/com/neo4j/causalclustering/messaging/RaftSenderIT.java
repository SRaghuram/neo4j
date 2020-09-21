/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.ServerLogService;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.consensus.protocol.RaftProtocolClientInstaller;
import com.neo4j.causalclustering.core.consensus.protocol.RaftProtocolServerInstaller;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.marshalling.DecodingDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageComposer;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import com.neo4j.causalclustering.messaging.marshalling.v2.SupportedMessagesV2;
import com.neo4j.causalclustering.messaging.marshalling.v3.SupportedMessagesV3;
import com.neo4j.causalclustering.messaging.marshalling.v3.decoding.RaftMessageDecoderV3;
import com.neo4j.causalclustering.messaging.marshalling.v3.encoding.RaftMessageEncoderV3;
import com.neo4j.causalclustering.messaging.marshalling.v4.decoding.RaftMessageDecoderV4;
import com.neo4j.causalclustering.messaging.marshalling.v4.encoding.RaftMessageEncoderV4;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.PooledChannel;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.init.ClientChannelInitializer;
import com.neo4j.causalclustering.protocol.init.ServerChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.causalclustering.messaging.marshalling.SupportedMessages.SUPPORT_ALL;
import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.RAFT;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

@ExtendWith( LifeExtension.class )
class RaftSenderIT
{
    @Inject
    private LifeSupport life;

    private final LogProvider logProvider = NullLogProvider.getInstance();

    private final ApplicationSupportedProtocols supportedApplicationProtocol = new ApplicationSupportedProtocols( RAFT,
            ApplicationProtocols.withCategory( RAFT ).stream().map( Protocol::implementation ).collect( toList() ) );
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols = emptyList();

    private final Duration handshakeTimeout = Duration.ofSeconds( 20 );

    private final ApplicationProtocolRepository clientProtocolRepository =
            new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedApplicationProtocol );
    private final ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

    private static ThreadPoolJobScheduler scheduler;
    private static ExecutorService serverExecutor;

    @BeforeAll
    static void setUp()
    {
        scheduler = new ThreadPoolJobScheduler();
        serverExecutor = Executors.newCachedThreadPool();
    }

    @AfterAll
    static void cleanUp() throws Exception
    {
        executeAll( scheduler::shutdown, () ->
        {
            serverExecutor.shutdownNow();
            assertTrue( serverExecutor.awaitTermination( 30, SECONDS ) );
        } );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldSendAndReceiveBlocking( ApplicationProtocols clientProtocol ) throws Exception
    {
        shouldSendAndReceive( clientProtocol, true );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldSendAndReceiveNonBlocking( ApplicationProtocols clientProtocol ) throws Exception
    {
        shouldSendAndReceive( clientProtocol, false );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldReturnSameChannelForMultipleRequests( ApplicationProtocols clientProtocol ) throws Exception
    {
        Server server = life.add( raftServer( new ChannelInboundHandlerAdapter(), clientProtocol ) );
        RaftChannelPoolService clientPool = life.add( raftPoolService( clientProtocol ) );

        Channel channelA = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        Channel channelB = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        assertEquals( channelA, channelB );

        Channel channelC = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        assertEquals( channelB, channelC );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldReturnNewChannelAfterClose( ApplicationProtocols clientProtocol ) throws Exception
    {
        Server server = life.add( raftServer( new ChannelInboundHandlerAdapter(), clientProtocol ) );
        RaftChannelPoolService clientPool = life.add( raftPoolService( clientProtocol ) );

        Channel channelA = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        channelA.close().sync();

        Channel channelB = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        assertNotEquals( channelA, channelB );
        channelB.close().sync();

        Channel channelC = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        assertNotEquals( channelB, channelC );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldCloseChannelOnCloseOfPool( ApplicationProtocols clientProtocol ) throws Exception
    {
        Server server = life.add( raftServer( new ChannelInboundHandlerAdapter(), clientProtocol ) );
        RaftChannelPoolService clientPool = life.add( raftPoolService( clientProtocol ) );

        Channel channelA = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        channelA.close().sync();

        Channel channelB = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();
        Channel channelC = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();

        clientPool.stop();
        clientPool.shutdown();

        assertFalse( channelA.isOpen() );
        assertFalse( channelB.isOpen() );
        assertFalse( channelC.isOpen() );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldReacquireAfterInitialFailure( ApplicationProtocols clientProtocol ) throws Exception
    {
        Server server = life.add( raftServer( new ChannelInboundHandlerAdapter(), clientProtocol ) );
        RaftChannelPoolService clientPool = life.add( raftPoolService( clientProtocol ) );

        server.stop();
        assertThrows( ExecutionException.class, () -> clientPool.acquire( server.address() ).get() );

        server.start();
        Channel channelB = clientPool.acquire( server.address() ).get( 10, SECONDS ).channel();

        assertTrue( channelB.isOpen() );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldAcquireInQuickSuccession( ApplicationProtocols clientProtocol ) throws Exception
    {
        Server server = life.add( raftServer( new ChannelInboundHandlerAdapter(), clientProtocol ) );
        RaftChannelPoolService clientPool = life.add( raftPoolService( clientProtocol ) );

        // this tries to be a bit racy, acquiring several times before the connection is fully up
        CompletableFuture<PooledChannel> fChannelA = clientPool.acquire( server.address() );
        CompletableFuture<PooledChannel> fChannelB = clientPool.acquire( server.address() );
        CompletableFuture<PooledChannel> fChannelC = clientPool.acquire( server.address() );

        Channel channelA = fChannelA.get( 10, SECONDS ).channel();
        Channel channelB = fChannelB.get( 10, SECONDS ).channel();
        Channel channelC = fChannelC.get( 10, SECONDS ).channel();

        assertTrue( channelA.isOpen() );
        assertTrue( channelB.isOpen() );
        assertTrue( channelC.isOpen() );

        assertEquals( channelA, channelB );
        assertEquals( channelB, channelC );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "RAFT_.+" )
    void shouldStartStopWithoutAcquiringAnything( ApplicationProtocols clientProtocol )
    {
        life.add( raftPoolService( clientProtocol ) );
    }

    private void shouldSendAndReceive( ApplicationProtocols clientProtocol, boolean blocking ) throws Exception
    {
        // given: raft server handler
        Semaphore messageReceived = new Semaphore( 0 );
        ChannelInboundHandler nettyHandler = new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                messageReceived.release();
            }
        };
        Server raftServer = life.add( raftServer( nettyHandler, clientProtocol ) );

        // given: raft messaging service
        RaftChannelPoolService raftPoolService = life.add( raftPoolService( clientProtocol ) );

        RaftSender sender = new RaftSender( logProvider, raftPoolService );

        // when
        SocketAddress to = new SocketAddress( raftServer.address().getHostname(), raftServer.address().getPort() );
        RaftMemberId memberId = IdFactory.randomRaftMemberId();
        RaftId raftId = IdFactory.randomRaftId();

        RaftMessages.NewEntry.Request newEntryMessage = new RaftMessages.NewEntry.Request( memberId, new MemberIdSet( asSet( memberId ) ) );
        RaftMessages.OutboundRaftMessageContainer<?> message = RaftMessages.OutboundRaftMessageContainer.of( raftId, newEntryMessage );

        sender.send( to, message, blocking );

        // then
        assertTrue( messageReceived.tryAcquire( 15, SECONDS ) );
    }

    private Server raftServer( ChannelInboundHandler nettyHandler, ApplicationProtocols protocol )
    {
        NettyPipelineBuilderFactory pipelineFactory = NettyPipelineBuilderFactory.insecure();

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> installer;
        final var clock = Clock.systemUTC();
        if ( protocol == ApplicationProtocols.RAFT_2_0 )
        {

            var factoryV2 = new RaftProtocolServerInstaller.Factory( nettyHandler,
                                                                     pipelineFactory,
                                                                     logProvider,
                                                                     c -> new DecodingDispatcher( c, logProvider, RaftMessageDecoder::new ),
                                                                     () -> new RaftMessageComposer( clock ),
                                                                     protocol );
            installer = new ProtocolInstallerRepository<>( List.of( factoryV2 ), ModifierProtocolInstaller.allServerInstallers );
        }
        else if ( protocol == ApplicationProtocols.RAFT_3_0 )
        {
            var factoryV3 = new RaftProtocolServerInstaller.Factory( nettyHandler,
                                                                     pipelineFactory,
                                                                     logProvider,
                                                                     c -> new DecodingDispatcher( c, logProvider, RaftMessageDecoderV3::new ),
                                                                     () -> new RaftMessageComposer( clock ),
                                                                     protocol );
            installer = new ProtocolInstallerRepository<>( List.of( factoryV3 ), ModifierProtocolInstaller.allServerInstallers );
        }
        else if ( protocol == ApplicationProtocols.RAFT_4_0 )
        {
            var factoryV4 = new RaftProtocolServerInstaller.Factory( nettyHandler,
                                                                     pipelineFactory,
                                                                     logProvider,
                                                                     c -> new DecodingDispatcher( c, logProvider, RaftMessageDecoderV4::new ),
                                                                     () -> new RaftMessageComposer( clock ),
                                                                     protocol );
            installer = new ProtocolInstallerRepository<>( List.of( factoryV4 ), ModifierProtocolInstaller.allServerInstallers );
        }
        else
        {
            throw new IllegalArgumentException( "Unknown protocol " + protocol );
        }

        HandshakeServerInitializer handshakeInitializer = new HandshakeServerInitializer( clientProtocolRepository, modifierProtocolRepository,
                installer, pipelineFactory, logProvider, Config.defaults() );

        ServerChannelInitializer channelInitializer = new ServerChannelInitializer( handshakeInitializer, pipelineFactory, handshakeTimeout, logProvider,
                Config.defaults() );

        SocketAddress listenAddress = new SocketAddress( "localhost", 0 );

        return new Server( channelInitializer, null, new ServerLogService( logProvider, logProvider, "raft-server" ), listenAddress, "raft-server",
                serverExecutor,
                new ConnectorPortRegister(), BootstrapConfiguration.serverConfig( Config.defaults() ) );
    }

    private RaftChannelPoolService raftPoolService( ApplicationProtocols clientProtocol )
    {
        NettyPipelineBuilderFactory pipelineFactory = NettyPipelineBuilderFactory.insecure();

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller;
        if ( clientProtocol == ApplicationProtocols.RAFT_2_0 )
        {
            var factoryV2 = new RaftProtocolClientInstaller.Factory( pipelineFactory,
                                                                     logProvider,
                                                                     new SupportedMessagesV2(),
                                                                     () -> new RaftMessageEncoder(),
                                                                     ApplicationProtocols.RAFT_2_0 );
            protocolInstaller = new ProtocolInstallerRepository<>( Collections.singleton( factoryV2 ), ModifierProtocolInstaller.allClientInstallers );
        }
        else if ( clientProtocol == ApplicationProtocols.RAFT_3_0 )
        {
            var factoryV3 = new RaftProtocolClientInstaller.Factory( pipelineFactory,
                                                                     logProvider,
                                                                     new SupportedMessagesV3(),
                                                                     () -> new RaftMessageEncoderV3(),
                                                                     ApplicationProtocols.RAFT_3_0 );
            protocolInstaller = new ProtocolInstallerRepository<>( Collections.singleton( factoryV3 ), ModifierProtocolInstaller.allClientInstallers );
        }
        else if ( clientProtocol == ApplicationProtocols.RAFT_4_0 )
        {
            var factoryV4 = new RaftProtocolClientInstaller.Factory( pipelineFactory,
                                                                     logProvider,
                                                                     SUPPORT_ALL,
                                                                     () -> new RaftMessageEncoderV4(),
                                                                     ApplicationProtocols.RAFT_4_0 );
            protocolInstaller = new ProtocolInstallerRepository<>( Collections.singleton( factoryV4 ), ModifierProtocolInstaller.allClientInstallers );
        }
        else
        {
            throw new IllegalArgumentException( "Unexpected protocol " + clientProtocol );
        }

        HandshakeClientInitializer handshakeInitializer = new HandshakeClientInitializer( clientRepository( clientProtocol ),
                modifierProtocolRepository,
                protocolInstaller,
                pipelineFactory,
                handshakeTimeout,
                logProvider,
                logProvider );

        ClientChannelInitializer channelInitializer = new ClientChannelInitializer( handshakeInitializer, pipelineFactory, handshakeTimeout, logProvider );

        return new RaftChannelPoolService( BootstrapConfiguration.clientConfig( Config.defaults() ), scheduler, logProvider, channelInitializer );
    }

    private ApplicationProtocolRepository clientRepository( ApplicationProtocols clientProtocol )
    {
        return new ApplicationProtocolRepository( new ApplicationProtocols[]{clientProtocol},
                new ApplicationSupportedProtocols( RAFT, emptyList() ) );
    }
}
