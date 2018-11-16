/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.v1.CatchupProtocolServerInstallerV1;
import org.neo4j.causalclustering.catchup.v2.CatchupProtocolServerInstallerV2;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.net.ChildInitializer;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.scheduler.Group;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.CATCHUP;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

class TestCatchupServer extends Server
{
    TestCatchupServer( FileSystemAbstraction fileSystem, GraphDatabaseAPI graphDb, LogProvider logProvider, ExecutorService executor )
    {
        super( childInitializer( fileSystem, graphDb, logProvider ), logProvider, logProvider,
                new ListenSocketAddress( "localhost", PortAuthority.allocatePort() ), "fake-catchup-server", executor );
    }

    private static ChildInitializer childInitializer( FileSystemAbstraction fileSystem, GraphDatabaseAPI graphDb, LogProvider logProvider )
    {
        ApplicationSupportedProtocols catchupProtocols = new ApplicationSupportedProtocols( CATCHUP, emptyList() );
        ModifierSupportedProtocols modifierProtocols = new ModifierSupportedProtocols( COMPRESSION, emptyList() );

        ApplicationProtocolRepository catchupRepository = new ApplicationProtocolRepository( ApplicationProtocols.values(), catchupProtocols );
        ModifierProtocolRepository modifierRepository = new ModifierProtocolRepository( ModifierProtocols.values(), singletonList( modifierProtocols ) );

        Supplier<CheckPointer> checkPointer = () -> graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
        BooleanSupplier availability = () -> graphDb.getDependencyResolver().resolveDependency( DatabaseAvailabilityGuard.class ).isAvailable();
        Supplier<Database> dataSource = () -> graphDb.getDependencyResolver().resolveDependency( Database.class );

        org.neo4j.storageengine.api.StoreId kernelStoreId = dataSource.get().getStoreId();
        StoreId storeId = new StoreId( kernelStoreId.getCreationTime(), kernelStoreId.getRandomId(), kernelStoreId.getUpgradeTime(),
                kernelStoreId.getUpgradeId() );

        CheckPointerService checkPointerService = new CheckPointerService( checkPointer, createInitialisedScheduler(), Group.CHECKPOINT );
        RegularCatchupServerHandler catchupServerHandler = new RegularCatchupServerHandler( new Monitors(), logProvider,
                () -> storeId, dataSource, availability, fileSystem, null, checkPointerService );

        NettyPipelineBuilderFactory pipelineBuilder = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );

        List<ProtocolInstaller.Factory<ProtocolInstaller.Orientation.Server,?>> protocolInstallers = Arrays.asList(
                new CatchupProtocolServerInstallerV1.Factory( pipelineBuilder, logProvider, catchupServerHandler, DEFAULT_DATABASE_NAME ),
                new CatchupProtocolServerInstallerV2.Factory( pipelineBuilder, logProvider, catchupServerHandler )
        );

        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository = new ProtocolInstallerRepository<>(
                protocolInstallers, ModifierProtocolInstaller.allServerInstallers );

        return new HandshakeServerInitializer( catchupRepository, modifierRepository, protocolInstallerRepository, pipelineBuilder, logProvider );
    }
}
