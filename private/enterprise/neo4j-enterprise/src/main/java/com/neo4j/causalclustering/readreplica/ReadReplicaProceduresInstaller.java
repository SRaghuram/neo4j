/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaRoleProcedure;
import com.neo4j.causalclustering.discovery.procedures.ReadReplicaToggleProcedure;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.dbms.QuarantineOperator;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.procedures.ProceduresInstaller;
import com.neo4j.dbms.procedures.QuarantineProcedure;

import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

public class ReadReplicaProceduresInstaller extends ProceduresInstaller.CommonProceduresInstaller
{
    private final QuarantineOperator quarantineOperator;

    public ReadReplicaProceduresInstaller( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager,
            StandaloneDbmsReconcilerModule reconcilerModule, GlobalModule globalModule, ServerIdentity identityModule,
            InstalledProtocolHandler installedProtocolHandler, CatchupClientFactory catchupClientFactory, TopologyService topologyService,
            QuarantineOperator quarantineOperator )
    {
        super( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule, installedProtocolHandler, catchupClientFactory,
                topologyService, Stream::of );
        this.quarantineOperator = quarantineOperator;
    }

    @Override
    public void register() throws KernelException
    {
        registerClusterSpecificProcedures();
        globalProcedures.register( new QuarantineProcedure( quarantineOperator, globalModule.getGlobalClock(),
                globalModule.getGlobalConfig().get( GraphDatabaseSettings.db_timezone ).getZoneId() ) );

        globalProcedures.register( new ReadReplicaRoleProcedure( databaseManager ) );
        globalProcedures.register( new ReadReplicaToggleProcedure( databaseManager ) );
    }

}
