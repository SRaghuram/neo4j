/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.txtracking;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@EnterpriseDbmsExtension
class StandaloneReconciledTransactionTrackerIT
{
    @Inject
    private DatabaseManagementService dbService;

    private ReconciledTransactionTracker tracker;
    private TransactionIdStore txIdStore;

    @BeforeEach
    void beforeEach()
    {
        tracker = resolveFromSystemDb( ReconciledTransactionTracker.class );
        txIdStore = resolveFromSystemDb( TransactionIdStore.class );
    }

    @Test
    void shouldInitializeReconciledTransactionIdAfterStart() throws Exception
    {
        var lastClosedSystemTxId = txIdStore.getLastClosedTransactionId();
        assertThat( lastClosedSystemTxId ).isGreaterThan( 0L );
        assertEventually( () -> tracker.getLastReconciledTransactionId(), equalityCondition( lastClosedSystemTxId ), 1, MINUTES );
    }

    @Test
    void shouldUpdateReconciledTransactionId() throws Exception
    {
        var dbName1 = "foo";
        var dbName2 = "bar";
        var dbName3 = "baz";

        var lastClosedSystemTxIdBefore = txIdStore.getLastClosedTransactionId();

        dbService.createDatabase( dbName1 );
        dbService.createDatabase( dbName2 );
        dbService.createDatabase( dbName3 );
        dbService.dropDatabase( dbName2 );
        dbService.shutdownDatabase( dbName1 );

        var lastClosedSystemTxIdAfter = txIdStore.getLastClosedTransactionId();
        assertThat( lastClosedSystemTxIdAfter ).isGreaterThan( lastClosedSystemTxIdBefore );

        assertEventually( () -> tracker.getLastReconciledTransactionId(), equalityCondition( lastClosedSystemTxIdAfter ), 1, MINUTES );
    }

    private <T> T resolveFromSystemDb( Class<T> clazz )
    {
        var db = (GraphDatabaseAPI) dbService.database( SYSTEM_DATABASE_NAME );
        return db.getDependencyResolver().resolveDependency( clazz );
    }
}
