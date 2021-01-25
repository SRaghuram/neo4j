/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.dbms.database.ClusteredMultiDatabaseManager;

import java.util.stream.Stream;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.graphdb.factory.module.GlobalModule;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED_DUMPED;
import static com.neo4j.dbms.EnterpriseOperatorState.QUARANTINED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STORE_COPYING;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

public final class ClusteredDbmsReconcilerModule extends StandaloneDbmsReconcilerModule
{
    private final ReplicatedDatabaseEventService databaseEventService;
    private final ClusterInternalDbmsOperator internalOperator;
    private final SystemOperatingDatabaseEventListener operatorEventListener;
    private final QuarantineOperator quarantineOperator;

    public ClusteredDbmsReconcilerModule( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
                                         ReplicatedDatabaseEventService databaseEventService, ClusterStateStorageFactory stateStorageFactory,
                                         ReconciledTransactionTracker reconciledTxTracker, ClusterSystemGraphDbmsModel dbmsModel,
                                         QuarantineOperator quarantineOperator )
    {
        super( globalModule, databaseManager, reconciledTxTracker,
                createReconciler( globalModule, databaseManager, stateStorageFactory, quarantineOperator ), dbmsModel );
        this.databaseEventService = databaseEventService;
        this.internalOperator = databaseManager.internalDbmsOperator();
        this.quarantineOperator = quarantineOperator;
        this.operatorEventListener = new SystemOperatingDatabaseEventListener( systemOperator );
    }

    public void registerDatabaseStateChangedListener( DatabaseStateChangedListener databaseStateChangedListener )
    {
        reconciler.registerDatabaseStateChangedListener( databaseStateChangedListener );
    }

    @Override
    protected Stream<DbmsOperator> operators()
    {
        return Stream.concat( super.operators(), Stream.of( internalOperator, quarantineOperator ) );
    }

    @Override
    protected void registerWithListenerService( GlobalModule globalModule )
    {
        databaseEventService.registerListener( NAMED_SYSTEM_DATABASE_ID, operatorEventListener );
    }

    @Override
    protected void unregisterWithListenerService( GlobalModule globalModule )
    {
        databaseEventService.unregisterListener( NAMED_SYSTEM_DATABASE_ID, operatorEventListener );
    }

    static TransitionsTable createTransitionsTable( ClusterReconcilerTransitions t )
    {
        var standaloneTransitionsTable = StandaloneDbmsReconcilerModule.createTransitionsTable( t );
        TransitionsTable clusteredTransitionsTable = TransitionsTable.builder()
                .from( DIRTY ).to( DROPPED ).doTransitions( t.ensureDirtyDatabaseExists(), t.drop() )
                .from( DIRTY ).to( DROPPED_DUMPED ).doTransitions( t.ensureDirtyDatabaseExists(), t.dropDumpData() )
                // No prepareDrop step needed here as the database will be stopped for store copying anyway
                .from( STORE_COPYING ).to( DROPPED ).doTransitions( t.stop(), t.drop() )
                // Some Cluster components still need stopped when store copying.
                //   This will attempt to stop the kernel database again, but that should be idempotent.
                .from( STORE_COPYING ).to( STOPPED ).doTransitions( t.stop() )
                .from( STORE_COPYING ).to( STARTED ).doTransitions( t.startAfterStoreCopy() )
                .from( STARTED ).to( STORE_COPYING ).doTransitions( t.stopBeforeStoreCopy() )
                // Quarantine
                .from( STOPPED ).to( QUARANTINED ).doTransitions( t.setQuarantine() )
                .from( STARTED ).to( QUARANTINED ).doTransitions( t.stop(), t.setQuarantine() )
                .from( STORE_COPYING ).to( QUARANTINED ).doTransitions( t.stop(), t.setQuarantine() )
                .from( DIRTY ).to( QUARANTINED ).doTransitions( t.setQuarantine() )
                .from( QUARANTINED ).to( STARTED ).doTransitions( t.removeQuarantine(), t.validate(), t.create(), t.start() )
                .from( QUARANTINED ).to( STOPPED ).doTransitions( t.removeQuarantine(), t.validate(), t.create() )
                .from( QUARANTINED ).to( DROPPED ).doTransitions( t.removeQuarantine(), t.create(), t.drop() )
                .from( QUARANTINED ).to( DROPPED_DUMPED ).doTransitions( t.removeQuarantine(), t.create(), t.dropDumpData() )
                .build();

        return standaloneTransitionsTable.extendWith( clusteredTransitionsTable );
    }

    private static ClusteredDbmsReconciler createReconciler( GlobalModule globalModule, ClusteredMultiDatabaseManager databaseManager,
                                                            ClusterStateStorageFactory stateStorageFactory, QuarantineOperator quarantineOperator )
    {
        var logProvider = globalModule.getLogService().getInternalLogProvider();
        var transitionsTable = createTransitionsTable( new ClusterReconcilerTransitions( databaseManager, quarantineOperator ) );
        return new ClusteredDbmsReconciler( databaseManager, globalModule.getGlobalConfig(), logProvider, globalModule.getJobScheduler(),
                stateStorageFactory, transitionsTable, quarantineOperator );
    }
}
