/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.graphdb.factory.module.GlobalModule;

import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public class ClusteredDbmsReconcilerModule extends StandaloneDbmsReconcilerModule
{
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterInternalDbmsOperator internalOperator;

    public ClusteredDbmsReconcilerModule( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
            ReplicatedDatabaseEventService databaseEventService, ClusterStateStorageFactory stateStorageFactory,
            ReconciledTransactionTracker reconciledTxTracker, PanicService panicService, ClusterSystemGraphDbmsModel dbmsModel )
    {
        super( globalModule, databaseManager, reconciledTxTracker,
                createReconciler( globalModule, databaseManager, stateStorageFactory, panicService ), dbmsModel );
        this.databaseEventService = databaseEventService;
        this.internalOperator = databaseManager.internalDbmsOperator();
    }

    @Override
    protected Stream<DbmsOperator> operators()
    {
        return Stream.concat( super.operators(), Stream.of( internalOperator ) );
    }

    @Override
    protected void registerWithListenerService( GlobalModule globalModule, SystemGraphDbmsOperator systemOperator )
    {
        databaseEventService.registerListener( NAMED_SYSTEM_DATABASE_ID, new SystemOperatingDatabaseEventListener( systemOperator ) );
    }

    private static ClusteredDbmsReconciler createReconciler( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
            ClusterStateStorageFactory stateStorageFactory, PanicService panicService )
    {
        return new ClusteredDbmsReconciler( databaseManager, globalModule.getGlobalConfig(), globalModule.getLogService().getInternalLogProvider(),
                globalModule.getJobScheduler(), stateStorageFactory, panicService );
    }
}
