/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.tx.TxPullRequestBatchingLogger;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolServerInstallerV4;
import com.neo4j.causalclustering.catchup.v5.CatchupProtocolServerInstallerV5;
import com.neo4j.causalclustering.core.ServerNameService;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.ChildInitializer;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import com.neo4j.causalclustering.protocol.init.ServerChannelInitializer;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.causalclustering.protocol.modifier.ModifierProtocolCategory.COMPRESSION;
import static com.neo4j.configuration.CausalClusteringInternalSettings.experimental_catchup_protocol;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

class TestCatchupServer extends Server
{
    TestCatchupServer( CatchupServerHandler catchupServerHandler, LogProvider logProvider, ExecutorService executor )
    {
        super( childInitializer( catchupServerHandler, logProvider ), null, new ServerNameService( logProvider, logProvider, "fake-catchup-server" ),
                new SocketAddress( "localhost", 0 ), executor,
                new ConnectorPortRegister(), BootstrapConfiguration.serverConfig( Config.defaults() ) );
    }

    private static ChildInitializer childInitializer( CatchupServerHandler catchupServerHandler, LogProvider logProvider )
    {
        Config config = Config.newBuilder().set( experimental_catchup_protocol, true ).build();
        ApplicationSupportedProtocols catchupProtocols = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
        ModifierSupportedProtocols modifierProtocols = new ModifierSupportedProtocols( COMPRESSION, emptyList() );

        ApplicationProtocolRepository catchupRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
        ModifierProtocolRepository modifierRepository = new ModifierProtocolRepository( ModifierProtocols.values(), singletonList( modifierProtocols ) );

        NettyPipelineBuilderFactory pipelineBuilder = NettyPipelineBuilderFactory.insecure();

        var txPullRqLogger = new TxPullRequestBatchingLogger(
                logProvider, JobSchedulerFactory.createInitialisedScheduler(), Duration.ofSeconds( 30 ) );

        List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> protocolInstallers = List.of(
                new CatchupProtocolServerInstallerV3.Factory( pipelineBuilder, logProvider, catchupServerHandler, txPullRqLogger ),
                new CatchupProtocolServerInstallerV4.Factory( pipelineBuilder, logProvider, catchupServerHandler, txPullRqLogger ),
                new CatchupProtocolServerInstallerV5.Factory( pipelineBuilder, logProvider, catchupServerHandler, txPullRqLogger )
        );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                protocolInstallers, ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer handshakeInitializer = new HandshakeServerInitializer( catchupRepository, modifierRepository, protocolInstallerRepository,
                pipelineBuilder, logProvider, config );

        var handshakeTimeout = Duration.ofSeconds( 60 );

        return new ServerChannelInitializer( handshakeInitializer, pipelineBuilder, handshakeTimeout, logProvider, config );
    }
}
