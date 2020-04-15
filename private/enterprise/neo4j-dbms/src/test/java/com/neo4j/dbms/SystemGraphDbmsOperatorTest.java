/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.bolt.txtracking.DefaultReconciledTransactionTracker;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.internal.NullLogService;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class SystemGraphDbmsOperatorTest
{
    private NamedDatabaseId databaseOne = DatabaseIdFactory.from( "one", UUID.randomUUID() );
    private Map<NamedDatabaseId,DatabaseState> states = new HashMap<>();

    @Test
    void explicitRequestsForFailedDatabasesShouldBePriority()
    {
        setState( databaseOne, STARTED, true );

        var operator = setup( Set.of(), Set.of( databaseOne ) );
        var connector = new NoOperatorConnector();
        operator.connect( connector );
        operator.transactionCommitted( 1L, mock( TransactionData.class ) );

        Assertions.assertTrue( connector.request.isPriorityRequestForDatabase( databaseOne.name() ) );
    }

    @Test
    void explicitRequestsForDirtyFailedDatabasesShouldNotBePriority()
    {
        setState( databaseOne, DIRTY, true );

        var operator = setup( Set.of(), Set.of( databaseOne ) );
        var connector = new NoOperatorConnector();
        operator.connect( connector );
        operator.transactionCommitted( 1L, mock( TransactionData.class ) );

        Assertions.assertTrue( connector.request.isSimple() );
    }

    private SystemGraphDbmsOperator setup( Set<NamedDatabaseId> changed, Set<NamedDatabaseId> touched )
    {
        var dbmsModel = new NoEnterpriseSystemGraphDbmsModel( changed, touched );
        var databaseStateService = new StubDatabaseStateService( states );
        var transactionTracker = new DefaultReconciledTransactionTracker( NullLogService.getInstance() );
        return new SystemGraphDbmsOperator( dbmsModel, transactionTracker, databaseStateService, nullLogProvider() );
    }

    private void setState( NamedDatabaseId databaseId, EnterpriseOperatorState operatorState, boolean failed )
    {
        var state = new EnterpriseDatabaseState( databaseId, operatorState );
        if ( failed )
        {
            state = state.failed( new Exception() );
        }
        states.put( databaseId, state );
    }

    private static class NoEnterpriseSystemGraphDbmsModel extends EnterpriseSystemGraphDbmsModel
    {
        private final Set<NamedDatabaseId> changed;
        private final Set<NamedDatabaseId> touched;

        NoEnterpriseSystemGraphDbmsModel( Set<NamedDatabaseId> changed, Set<NamedDatabaseId> touched )
        {
            super( null );
            this.changed = changed;
            this.touched = touched;
        }

        @Override
        DatabaseUpdates updatedDatabases( TransactionData transactionData )
        {
            return new DatabaseUpdates( changed, touched );
        }

        @Override
        Map<String,EnterpriseDatabaseState> getDatabaseStates()
        {
            return Map.of();
        }
    }

    private static class NoOperatorConnector extends OperatorConnector
    {
        ReconcilerRequest request;

        NoOperatorConnector()
        {
            super( null );
        }

        public ReconcilerResult trigger( ReconcilerRequest request )
        {
            this.request = request;
            return new ReconcilerResult( Collections.emptyMap() );
        }
    }
}
