/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import com.neo4j.causalclustering.net.BootstrapConfiguration;
import com.neo4j.causalclustering.net.ChildInitializer;
import com.neo4j.causalclustering.net.Server;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import com.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import com.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import com.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static com.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

class TestCatchupServer extends Server
{
    TestCatchupServer( CatchupServerHandler catchupServerHandler, LogProvider logProvider, ExecutorService executor )
    {
        super( childInitializer( catchupServerHandler, logProvider ), logProvider, logProvider,
                new ListenSocketAddress( "localhost", 0 ), "fake-catchup-server", executor,
                BootstrapConfiguration.serverConfig( Config.defaults() ) );
    }

    private static ChildInitializer childInitializer( CatchupServerHandler catchupServerHandler, LogProvider logProvider )
    {
        ApplicationSupportedProtocols catchupProtocols = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
        ModifierSupportedProtocols modifierProtocols = new ModifierSupportedProtocols( COMPRESSION, emptyList() );

        ApplicationProtocolRepository catchupRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
        ModifierProtocolRepository modifierRepository = new ModifierProtocolRepository( ModifierProtocols.values(), singletonList( modifierProtocols ) );

        NettyPipelineBuilderFactory pipelineBuilder = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );

        List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> protocolInstallers = Arrays.asList(
                new CatchupProtocolServerInstallerV3.Factory( pipelineBuilder, logProvider, catchupServerHandler )
        );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                protocolInstallers, ModifierProtocolInstaller.allServerInstallers );

        return new HandshakeServerInitializer( catchupRepository, modifierRepository, protocolInstallerRepository, pipelineBuilder, logProvider );
    }
}
