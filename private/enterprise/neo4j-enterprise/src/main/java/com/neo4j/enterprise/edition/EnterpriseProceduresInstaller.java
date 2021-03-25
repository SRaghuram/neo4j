/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.v4.info.InfoProvider;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.AlwaysLeaderRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.InstalledProtocolsProcedure;
import com.neo4j.causalclustering.discovery.procedures.StandaloneClusterOverviewProcedure;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.procedures.ClusterSetDefaultDatabaseProcedure;
import com.neo4j.dbms.procedures.ProceduresInstaller;
import com.neo4j.dbms.procedures.wait.WaitProcedure;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInDbmsProcedures;
import com.neo4j.procedure.enterprise.builtin.EnterpriseBuiltInProcedures;

import java.util.UUID;
import java.util.stream.Stream;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.dbms.procedures.StandaloneDatabaseStateProcedure;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

public class EnterpriseProceduresInstaller extends ProceduresInstaller.CommonProceduresInstaller
{
    public EnterpriseProceduresInstaller( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager,
            StandaloneDbmsReconcilerModule reconcilerModule, GlobalModule globalModule, ServerIdentity identityModule,
            InstalledProtocolHandler installedProtocolHandler, CatchupClientFactory catchupClientFactory, TopologyService topologyService )
    {
        super( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule, installedProtocolHandler, catchupClientFactory,
                topologyService, Stream::of );
    }

    @Override
    public void register() throws KernelException
    {
        if ( EnterpriseEditionModule.clusteringEnabled( globalModule.getGlobalConfig() ) )
        {
            registerClusterSpecificProcedures();
            globalProcedures.register( new ClusterSetDefaultDatabaseProcedure( databaseManager.databaseIdRepository(), topologyService ) );
        }
        else
        {
            registerStandaloneSpecificProcedures();
        }
        globalProcedures.register( new AlwaysLeaderRoleProcedure( databaseManager ) );
    }

    private void registerStandaloneSpecificProcedures() throws KernelException
    {
        globalProcedures.registerProcedure( EnterpriseBuiltInDbmsProcedures.class, true );
        globalProcedures.registerProcedure( EnterpriseBuiltInProcedures.class, true );
        globalProcedures.register( new StandaloneClusterOverviewProcedure( identityModule.serverId(), globalModule.getGlobalConfig(), databaseManager ) );
        globalProcedures.register( new InstalledProtocolsProcedure( Stream::of, installedProtocolHandler::installedProtocols ) );
        globalProcedures.register( new StandaloneDatabaseStateProcedure( reconcilerModule.databaseStateService(),
                databaseManager.databaseIdRepository(), globalModule.getGlobalConfig().get( BoltConnector.advertised_address ).toString() ) );
        globalProcedures.register(
                WaitProcedure.standalone( new ServerId( new UUID( 0, 1 ) ), globalModule.getGlobalConfig().get( BoltConnector.advertised_address ),
                        globalModule.getGlobalClock(), globalModule.getLogService().getInternalLogProvider(),
                        new InfoProvider( databaseManager, reconcilerModule.databaseStateService() ) ) );
    }
}
