/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;
import com.neo4j.dbms.database.DatabaseOperationCountMonitor;

import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public final class ClusteredDbmsReconcilerModule extends StandaloneDbmsReconcilerModule
{
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterInternalDbmsOperator internalOperator;

    public static ClusteredDbmsReconcilerModule create( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
            ReplicatedDatabaseEventService databaseEventService, ClusterStateStorageFactory stateStorageFactory,
            ReconciledTransactionTracker reconciledTxTracker, PanicService panicService, ClusterSystemGraphDbmsModel dbmsModel )
    {
        var monitor = globalModule.getGlobalMonitors().newMonitor( DatabaseOperationCountMonitor.class, StandaloneDbmsReconcilerModule.class.getName() );
        var logProvider = globalModule.getLogService().getInternalLogProvider();
        var transitionsTable = createTransitionsTable( new ClusterReconcilerTransitions( databaseManager, logProvider, monitor ) );

        var reconciler = createReconciler( globalModule, databaseManager, stateStorageFactory, logProvider, panicService, transitionsTable );
        return new ClusteredDbmsReconcilerModule( globalModule, databaseManager, databaseEventService, reconciledTxTracker, dbmsModel, reconciler, monitor );
    }

    private ClusteredDbmsReconcilerModule( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
            ReplicatedDatabaseEventService databaseEventService, ReconciledTransactionTracker reconciledTxTracker, ClusterSystemGraphDbmsModel dbmsModel,
            DbmsReconciler reconciler, DatabaseOperationCountMonitor monitor )
    {
        super( globalModule, databaseManager, reconciledTxTracker, reconciler, dbmsModel, monitor );
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

    static TransitionsTable createTransitionsTable( ClusterReconcilerTransitions t )
    {
        var standaloneTransitionsTable = StandaloneDbmsReconcilerModule.createTransitionsTable( t );
        TransitionsTable clusteredTransitionsTable = TransitionsTable.builder()
                // All transitions from UNKNOWN to $X get deconstructed into UNKNOWN -> DROPPED -> $X
                //     inside Transitions so only actions for this from/to pair need to be specified
                .from( UNKNOWN ).to( DROPPED ).doTransitions( t.logCleanupAndDrop() )
                // No prepareDrop step needed here as the database will be stopped for store copying anyway
                .from( STORE_COPYING ).to( DROPPED ).doTransitions( t.stop(), t.drop() )
                // Some Cluster components still need stopped when store copying.
                //   This will attempt to stop the kernel database again, but that should be idempotent.
                .from( STORE_COPYING ).to( STOPPED ).doTransitions( t.stop() )
                .from( STORE_COPYING ).to( STARTED ).doTransitions( t.startAfterStoreCopy() )
                .from( STARTED ).to( STORE_COPYING ).doTransitions( t.stopBeforeStoreCopy() )
                .build();

        return standaloneTransitionsTable.extendWith( clusteredTransitionsTable );
    }

    private static ClusteredDbmsReconciler createReconciler( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
            ClusterStateStorageFactory stateStorageFactory, LogProvider logProvider, PanicService panicService, TransitionsTable transitionsTable )
    {
        return new ClusteredDbmsReconciler( databaseManager, globalModule.getGlobalConfig(), logProvider, globalModule.getJobScheduler(),
                stateStorageFactory, panicService, transitionsTable );
    }
}
