/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.v4.info.InfoProvider;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import com.neo4j.causalclustering.discovery.procedures.TopologyBasedClusterOverviewProcedure;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.procedures.wait.WaitProcedure;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInProcedures;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

public interface ProceduresInstaller
{
    void register() throws KernelException;

    abstract class CommonProceduresInstaller implements ProceduresInstaller
    {
        protected final GlobalProcedures globalProcedures;
        protected final DatabaseManager<?> databaseManager;
        protected final StandaloneDbmsReconcilerModule reconcilerModule;
        protected final GlobalModule globalModule;
        protected final ServerIdentity identityModule;
        protected final InstalledProtocolHandler installedProtocolHandler;
        protected final CatchupClientFactory catchupClientFactory;
        protected final TopologyService topologyService;
        protected final Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> clientInstalledProtocols;

        protected CommonProceduresInstaller( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager,
                StandaloneDbmsReconcilerModule reconcilerModule, GlobalModule globalModule, ServerIdentity identityModule,
                InstalledProtocolHandler installedProtocolHandler, CatchupClientFactory catchupClientFactory, TopologyService topologyService,
                Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> clientInstalledProtocols )
        {
            this.globalProcedures = globalProcedures;
            this.databaseManager = databaseManager;
            this.reconcilerModule = reconcilerModule;
            this.globalModule = globalModule;
            this.identityModule = identityModule;
            this.installedProtocolHandler = installedProtocolHandler;
            this.catchupClientFactory = catchupClientFactory;
            this.topologyService = topologyService;
            this.clientInstalledProtocols = clientInstalledProtocols;
        }

        protected void registerClusterSpecificProcedures() throws KernelException
        {
            globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
            globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
            globalProcedures.register( new TopologyBasedClusterOverviewProcedure( topologyService, databaseManager.databaseIdRepository() ) );
            globalProcedures.register( new InstalledProtocolsProcedure( clientInstalledProtocols, installedProtocolHandler::installedProtocols ) );
            globalProcedures.register( new ClusteredDatabaseStateProcedure( databaseManager.databaseIdRepository(), topologyService ) );
            globalProcedures.register(
                    WaitProcedure.clustered( topologyService, identityModule, globalModule.getGlobalClock(),
                            catchupClientFactory, globalModule.getLogService().getInternalLogProvider(),
                            new InfoProvider( databaseManager, reconcilerModule.databaseStateService() ) ) );
            globalProcedures.registerComponent( DatabaseManager.class, ignored -> databaseManager, false );
        }
    }
}

