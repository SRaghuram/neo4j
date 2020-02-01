/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.UNKNOWN;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;
import static org.neo4j.test.conditions.Conditions.FALSE;
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
    void shouldPanicDatabaseThatFailsToTransitionToDesiredState()
    {
        // given
        // a fake operator that desires a state invalid for a standalone database
        var invalidDesiredState = new EnterpriseDatabaseState( db.databaseId(), UNKNOWN );
        var fixedOperator = new FixedDbmsOperator( Map.of( db.databaseName(), invalidDesiredState ) );

        // when
        // reconciler fails to reconcile the state transition
        var reconcilerResult = reconciler.reconcile( List.of( fixedOperator ), ReconcilerRequest.simple() );

        // then
        var error = assertThrows( CompletionException.class, () -> reconcilerResult.await( db.databaseId() ) );
        assertThat( error.getCause().getMessage(), containsString( "unsupported state transition" ) );
        var dbHealth = db.getDependencyResolver().resolveDependency( DatabaseHealth.class );
        assertEventually( "Database is expected to panic", dbHealth::isHealthy, FALSE, 30, SECONDS );
    }

    @Test
    void shouldStillBeAbleToForceReconcileFailedDatabase() throws Exception
    {
        // given
        // a fake operator that desires a state invalid for a standalone database
        var invalidDesiredState = new EnterpriseDatabaseState( db.databaseId(), UNKNOWN );
        var fixedOperator = new FixedDbmsOperator( Map.of( db.databaseName(), invalidDesiredState ) );

        // a failed database
        var reconcilerResult = reconciler.reconcile( List.of( fixedOperator ), ReconcilerRequest.simple() );
        assertThrows( CompletionException.class, () -> reconcilerResult.await( db.databaseId() ) );
        assertTrue( reconciler.causeOfFailure( db.databaseId() ).isPresent(), "Database is expected to be failed" );

        // when
        localOperator.stopDatabase( db.databaseName() );

        assertEventually( "Database should be stopped",
                () -> reconciler.stateOfDatabase( db.databaseId() ), equalityCondition( STOPPED ), 10, SECONDS );
        assertTrue( reconciler.causeOfFailure( db.databaseId() ).isEmpty(), "Database is *not* expected to be failed" );
    }

}
