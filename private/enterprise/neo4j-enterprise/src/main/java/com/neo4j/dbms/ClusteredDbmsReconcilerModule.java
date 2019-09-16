/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.ClusteredMultiDatabaseManager;
import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.error_handling.PanicService;

import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.graphdb.factory.module.GlobalModule;

import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

public class ClusteredDbmsReconcilerModule extends StandaloneDbmsReconcilerModule<ClusteredMultiDatabaseManager>
{
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterInternalDbmsOperator internalOperator;
    private final ClusterStateStorageFactory stateStorageFactory;
    private final PanicService panicService;

    public ClusteredDbmsReconcilerModule( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
            ReplicatedDatabaseEventService databaseEventService, ClusterInternalDbmsOperator internalOperator,
            ClusterStateStorageFactory stateStorageFactory, ReconciledTransactionTracker reconciledTxTracker, PanicService panicService )
    {
        super( globalModule, databaseManager, reconciledTxTracker );
        this.databaseEventService = databaseEventService;
        this.internalOperator = internalOperator;
        this.stateStorageFactory = stateStorageFactory;
        this.panicService = panicService;
        //TODO: don't need if we do end up injecting
        globalModule.getGlobalDependencies().satisfyDependencies( internalOperator );
    }

    @Override
    protected Stream<DbmsOperator> operators()
    {
        return Stream.concat( super.operators(), Stream.of( internalOperator ) );
    }

    @Override
    protected void registerWithListenerService( GlobalModule globalModule, SystemGraphDbmsOperator systemOperator )
    {
        databaseEventService.registerListener( SYSTEM_DATABASE_ID, new SystemOperatingDatabaseEventListener( systemOperator ) );
    }

    @Override
    protected ClusteredDbmsReconciler createReconciler( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager )
    {
        return new ClusteredDbmsReconciler( databaseManager, globalModule.getGlobalConfig(), globalModule.getLogService().getInternalLogProvider(),
                globalModule.getJobScheduler(), stateStorageFactory, panicService );
    }
}
