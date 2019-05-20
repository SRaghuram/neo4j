/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.helpers.ListenSocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;

class RaftSenderIT
{
    private final LogProvider logProvider = NullLogProvider.getInstance();

    private final ApplicationSupportedProtocols supportedApplicationProtocol = new ApplicationSupportedProtocols( RAFT,
            ApplicationProtocols.withCategory( RAFT ).stream().map( Protocol::implementation ).collect( toList() ) );
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols = emptyList();

    private final ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedApplicationProtocol );
    private final ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "^RAFT_\\d" )
    void shouldSendAndReceiveBlocking( ApplicationProtocols clientProtocol ) throws Throwable
    {
        shouldSendAndReceive( clientProtocol, true );
    }

    @ParameterizedTest
    @EnumSource( value = ApplicationProtocols.class, mode = EnumSource.Mode.MATCH_ALL, names = "^RAFT_\\d" )
    void shouldSendAndReceiveNonBlocking( ApplicationProtocols clientProtocol ) throws Throwable
    {
        shouldSendAndReceive( clientProtocol, false );
    }

    private void shouldSendAndReceive( ApplicationProtocols clientProtocol, boolean blocking ) throws Throwable
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
        Server raftServer = raftServer( nettyHandler );
        raftServer.start();

        // given: raft messaging service
        RaftChannelPoolService raftPoolService = raftPoolService( clientProtocol );
        raftPoolService.start();

        RaftSender sender = new RaftSender( logProvider, raftPoolService );

        // when
        AdvertisedSocketAddress to = new AdvertisedSocketAddress( raftServer.address().getHostname(), raftServer.address().getPort() );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );

        RaftMessages.NewEntry.Request newEntryMessage = new RaftMessages.NewEntry.Request( memberId, new MemberIdSet( asSet( memberId ) ) );
        RaftMessages.ClusterIdAwareMessage<?> message = RaftMessages.ClusterIdAwareMessage.of( clusterId, newEntryMessage );

        sender.send( to, message, blocking );

        // then
        assertTrue( messageReceived.tryAcquire( 15, SECONDS ) );

        // cleanup
        raftPoolService.stop();
        raftServer.stop();
    }

    private Server raftServer( ChannelInboundHandler nettyHandler )
    {
        NettyPipelineBuilderFactory pipelineFactory = new NettyPipelineBuilderFactory( VOID_WRAPPER );

        RaftProtocolServerInstallerV2.Factory factoryV2 =
                new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> installer =
                new ProtocolInstallerRepository<>( List.of( factoryV2 ), ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer channelInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                installer, pipelineFactory, logProvider );

        ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", 0 );
        ExecutorService serverExecutor = Executors.newCachedThreadPool();
        return new Server( channelInitializer, null, logProvider, logProvider, listenAddress, "raft-server", serverExecutor,
                new ConnectorPortRegister(), BootstrapConfiguration.serverConfig( Config.defaults() ) );
    }

    private RaftChannelPoolService raftPoolService( ApplicationProtocols clientProtocol )
    {
        NettyPipelineBuilderFactory pipelineFactory = new NettyPipelineBuilderFactory( VOID_WRAPPER );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller;
        if ( clientProtocol == ApplicationProtocols.RAFT_2 )
        {
            RaftProtocolClientInstallerV2.Factory factoryV2 = new RaftProtocolClientInstallerV2.Factory( pipelineFactory, logProvider );
            protocolInstaller = new ProtocolInstallerRepository<>( Collections.singleton( factoryV2 ), ModifierProtocolInstaller.allClientInstallers );
        }
        else
        {
            throw new IllegalArgumentException( "Unexpected protocol " + clientProtocol );
        }

        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer( clientRepository( clientProtocol ),
                modifierProtocolRepository,
                protocolInstaller,
                pipelineFactory,
                Duration.ofSeconds(5),
                logProvider,
                logProvider );

        return new RaftChannelPoolService( BootstrapConfiguration.clientConfig( Config.defaults() ), new ThreadPoolJobScheduler(), logProvider,
                channelInitializer );
    }

    private ApplicationProtocolRepository clientRepository( ApplicationProtocols clientProtocol )
    {
        return new ApplicationProtocolRepository( new ApplicationProtocols[]{clientProtocol},
                new ApplicationSupportedProtocols( RAFT, emptyList() ) );
    }
}
