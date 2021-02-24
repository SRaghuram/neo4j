/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.monitoring.DatabaseHealth;

import static com.neo4j.dbms.EnterpriseOperatorState.DIRTY;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@EnterpriseDbmsExtension
class DbmsReconcilerIT
{
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private DbmsReconciler reconciler;
    @Inject
    private DatabaseStateService databaseStateService;
    @Inject
    private LocalDbmsOperator localOperator;

    private DatabaseIdRepository idRepository;
    private GraphDatabaseAPI db;

    @BeforeEach
    void setup()
    {
        idRepository = new TestDatabaseIdRepository();
        var databaseId = idRepository.getById( randomDatabaseId() ).orElseThrow();
        var databaseName = databaseId.name();
        managementService.createDatabase( databaseName );
        db = (GraphDatabaseAPI) managementService.database( databaseName );
    }

    @Test
    void shouldStopAndFailDatabaseOnUnderlyingPanic()
    {
        // given
        var databaseHealth = db.getDependencyResolver().resolveDependency( DatabaseHealth.class );
        var err = new Exception( "Panic cause" );

        // when
        databaseHealth.panic( err );

        // then
        assertEventually( "Reconciler should eventually stop",
                () -> databaseStateService.stateOfDatabase( db.databaseId() ).operatorState(), equalityCondition( STOPPED ), 10, SECONDS );
        assertEquals( err, databaseStateService.causeOfFailure( db.databaseId() ).orElse( null ) );
    }

    @Test
    void shouldBeFailedDatabaseOnIncorrectTransition()
    {
        // given
        // a fake operator that desires a state invalid for a standalone database
        var invalidDesiredState = new EnterpriseDatabaseState( db.databaseId(), DIRTY );
        var fixedOperator = new FixedDbmsOperator( Map.of( db.databaseName(), invalidDesiredState ) );

        // when
        // reconciler fails to reconcile the state transition
        var reconcilerResult = reconciler.reconcile( List.of( fixedOperator ), ReconcilerRequest.simple() );

        // then
        var error = assertThrows( DatabaseManagementException.class, () -> reconcilerResult.join( db.databaseId() ) );
        assertTrue( Exceptions.contains( error, "unsupported state transition", IllegalArgumentException.class ) );
        assertEquals( EnterpriseOperatorState.STARTED, databaseStateService.stateOfDatabase( db.databaseId() ).operatorState() );
        assertTrue( databaseStateService.causeOfFailure( db.databaseId() ).isPresent() );
    }

    @Test
    void shouldStillBeAbleToForceReconcileFailedDatabase() throws Exception
    {
        // given
        // a fake operator that desires a state invalid for a standalone database
        var invalidDesiredState = new EnterpriseDatabaseState( db.databaseId(), DIRTY );
        var fixedOperator = new FixedDbmsOperator( Map.of( db.databaseName(), invalidDesiredState ) );

        // a failed database
        var reconcilerResult = reconciler.reconcile( List.of( fixedOperator ), ReconcilerRequest.simple() );
        assertThrows( DatabaseManagementException.class, () -> reconcilerResult.join( db.databaseId() ) );
        assertTrue( databaseStateService.causeOfFailure( db.databaseId() ).isPresent(), "Database is expected to be failed" );

        // when
        localOperator.stopDatabase( db.databaseName() );

        assertEventually( "Database should be stopped",
                () -> databaseStateService.stateOfDatabase( db.databaseId() ).operatorState(), equalityCondition( STOPPED ), 10, SECONDS );
        assertTrue( databaseStateService.causeOfFailure( db.databaseId() ).isEmpty(), "Database is *not* expected to be failed" );
    }
}
