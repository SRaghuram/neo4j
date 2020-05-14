/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.logging.internal.NullLogService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class SystemGraphDbmsOperatorTest
{

    @Test
    void touchedDatabasesShouldBeExplicitInReconcilerRequest()
    {
        // given
        var touchedDb = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var databaseUpdates = new DatabaseUpdates( Set.of(), Set.of(), Set.of( touchedDb ) );
        var systemGraphDbStates = Map.of(
                SYSTEM_DATABASE_NAME, new EnterpriseDatabaseState( NAMED_SYSTEM_DATABASE_ID, EnterpriseOperatorState.STARTED ),
                "foo", new EnterpriseDatabaseState( touchedDb, EnterpriseOperatorState.STARTED )
        );
        var dbmsModel = new StubEnterpriseSystemGraphDbmsModel( databaseUpdates, systemGraphDbStates );
        var transactionTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
        var operator = new SystemGraphDbmsOperator( dbmsModel, transactionTracker, nullLogProvider() );

        var dbmsReconciler = mock( DbmsReconciler.class );
        when( dbmsReconciler.reconcile( anyList(), any() ) ).thenReturn( ReconcilerResult.EMPTY );
        var connector = new TestOperatorConnector( dbmsReconciler );
        operator.connect( connector );

        // when
        operator.transactionCommitted( 1L, mock( TransactionData.class ) );

        // then
        var expectedCall = Pair.of( systemGraphDbStates, ReconcilerRequest.targets( Set.of( touchedDb ) ).build() );
        assertThat( connector.triggerCalls() ).contains( expectedCall );
    }

    @Test
    void droppedDatabasesShouldBePriorityInReconcilerRequest()
    {
        // given
        var touchedDb = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var databaseUpdates = new DatabaseUpdates( Set.of(), Set.of( touchedDb ), Set.of() );
        var systemGraphDbStates = Map.of(
                SYSTEM_DATABASE_NAME, new EnterpriseDatabaseState( NAMED_SYSTEM_DATABASE_ID, EnterpriseOperatorState.STARTED ),
                "foo", new EnterpriseDatabaseState( touchedDb, EnterpriseOperatorState.DROPPED )
        );
        var dbmsModel = new StubEnterpriseSystemGraphDbmsModel( databaseUpdates, systemGraphDbStates );
        var transactionTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
        var operator = new SystemGraphDbmsOperator( dbmsModel, transactionTracker, nullLogProvider() );

        var dbmsReconciler = mock( DbmsReconciler.class );
        when( dbmsReconciler.reconcile( anyList(), any() ) ).thenReturn( ReconcilerResult.EMPTY );
        var connector = new TestOperatorConnector( dbmsReconciler );
        operator.connect( connector );

        // when
        operator.transactionCommitted( 1L, mock( TransactionData.class ) );

        // then
        var expectedCall = Pair.of( systemGraphDbStates, ReconcilerRequest.priorityTargets( Set.of( touchedDb ) ).build() );
        assertThat( connector.triggerCalls() ).contains( expectedCall );
    }

    private static class StubEnterpriseSystemGraphDbmsModel extends EnterpriseSystemGraphDbmsModel
    {
        private final DatabaseUpdates databaseUpdates;
        private final Map<String,EnterpriseDatabaseState> systemGraphDbStates;

        StubEnterpriseSystemGraphDbmsModel( DatabaseUpdates databaseUpdates, Map<String,EnterpriseDatabaseState> systemGraphDbStates )
        {
            super( null );
            this.databaseUpdates = databaseUpdates;
            this.systemGraphDbStates = systemGraphDbStates;
        }

        @Override
        DatabaseUpdates updatedDatabases( TransactionData transactionData )
        {
            return databaseUpdates;
        }

        @Override
        Map<String,EnterpriseDatabaseState> getDatabaseStates()
        {
            return systemGraphDbStates;
        }
    }
}
