/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.OperatorState.STORE_COPYING;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.test.assertion.Assert.assertEventually;

@EnterpriseDbmsExtension
class DbmsReconcilerIT
{
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void shouldPanicDatabaseThatFailsToTransitionToDesiredState() throws Exception
    {
        var databaseId = TestDatabaseIdRepository.randomDatabaseId();
        var databaseName = databaseId.name();
        managementService.createDatabase( databaseName );
        var db = (GraphDatabaseAPI) managementService.database( databaseName );
        var reconciler = db.getDependencyResolver().resolveDependency( DbmsReconciler.class );

        // a fake operator that desires a state invalid for a standalone database
        var invalidDesiredState = new DatabaseState( db.databaseId(), STORE_COPYING );
        var fixedOperator = new FixedDbmsOperator( Map.of( databaseName, invalidDesiredState ) );

        // reconciler fails to reconcile the state transition
        var reconcilerResult = reconciler.reconcile( List.of( fixedOperator ), ReconcilerRequest.simple() );
        var error = assertThrows( CompletionException.class, () -> reconcilerResult.await( databaseId ) );
        assertThat( error.getCause().getMessage(), containsString( "unsupported state transition" ) );

        // database panicked
        var dbHealth = db.getDependencyResolver().resolveDependency( DatabaseHealth.class );
        assertEventually( "Database is expected to panic", dbHealth::isHealthy, equalTo( false ), 30, SECONDS );
    }

}
