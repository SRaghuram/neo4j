/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.core.replication.ReplicationBenchmarkProcedure;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.procedures.CoreRoleProcedure;
import com.neo4j.causalclustering.net.InstalledProtocolHandler;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.dbms.QuarantineOperator;
import com.neo4j.dbms.StandaloneDbmsReconcilerModule;
import com.neo4j.dbms.procedures.ClusterSetDefaultDatabaseProcedure;
import com.neo4j.dbms.procedures.ProceduresInstaller;
import com.neo4j.dbms.procedures.QuarantineProcedure;

import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.api.procedure.GlobalProcedures;

public class CoreProceduresInstaller extends ProceduresInstaller.CommonProceduresInstaller
{
    private final QuarantineOperator quarantineOperator;

    public CoreProceduresInstaller( GlobalProcedures globalProcedures, DatabaseManager<?> databaseManager,
            StandaloneDbmsReconcilerModule reconcilerModule, GlobalModule globalModule, ServerIdentity identityModule,
            InstalledProtocolHandler installedProtocolHandler, CatchupClientFactory catchupClientFactory, TopologyService topologyService,
            Supplier<Stream<Pair<SocketAddress,ProtocolStack>>> clientInstalledProtocols, QuarantineOperator quarantineOperator )
    {
        super( globalProcedures, databaseManager, reconcilerModule, globalModule, identityModule, installedProtocolHandler, catchupClientFactory,
                topologyService, clientInstalledProtocols );
        this.quarantineOperator = quarantineOperator;
    }

    @Override
    public void register() throws KernelException
    {
        registerClusterSpecificProcedures();
        globalProcedures.register( new QuarantineProcedure( quarantineOperator, globalModule.getGlobalClock(),
                globalModule.getGlobalConfig().get( GraphDatabaseSettings.db_timezone ).getZoneId() ) );

        globalProcedures.register( new CoreRoleProcedure( databaseManager ) );
        globalProcedures.registerProcedure( ReplicationBenchmarkProcedure.class );
        globalProcedures.register( new ClusterSetDefaultDatabaseProcedure( databaseManager.databaseIdRepository(), topologyService ) );
    }
}
