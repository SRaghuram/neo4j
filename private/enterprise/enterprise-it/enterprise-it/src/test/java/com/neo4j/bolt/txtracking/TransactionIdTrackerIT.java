/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.bolt.txtracking;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.bolt.txtracking.ReconciledTransactionTracker;
import org.neo4j.bolt.txtracking.TransactionIdTracker;
import org.neo4j.bolt.txtracking.TransactionIdTrackerException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.time.Clocks;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.NamedThreadFactory.daemon;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseNotFound;
import static org.neo4j.kernel.api.exceptions.Status.Database.DatabaseUnavailable;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.BookmarkTimeout;
import static org.neo4j.test.conditions.Conditions.TRUE;
import static org.neo4j.test.assertion.Assert.assertEventually;

@EnterpriseDbmsExtension
class TransactionIdTrackerIT
{
    @Inject
    private GraphDatabaseAPI nonSystemDb;
    @Inject
    private DatabaseManagementService dbService;

    private TransactionIdTracker tracker;
    private Monitors monitors;
    private ExecutorService executor;

    @BeforeEach
    void beforeEach()
    {
        monitors = new Monitors();
        tracker = new TransactionIdTracker( dbService, monitors, Clocks.nanoClock() );
    }

    @AfterEach
    void afterEach()
    {
        if ( executor != null )
        {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldReturnNewestTransactionIdForNonSystemDatabase()
    {
        var databaseId = databaseId( nonSystemDb );
        createNodes( nonSystemDb, 42 );

        var lastCommittedTxId = lastCommittedTxId( nonSystemDb );
        var newestTransactionId = tracker.newestTransactionId( databaseId );

        assertThat( lastCommittedTxId ).isGreaterThanOrEqualTo( 42L );
        assertEquals( lastCommittedTxId, newestTransactionId );
    }

    @Test
    void shouldReturnNewestTransactionIdForSystemDatabase()
    {
        var lastCommittedTxIdBefore = lastCommittedTxId( SYSTEM_DATABASE_NAME );

        createDatabases( 5 );

        var lastCommittedTxIdAfter = lastCommittedTxId( SYSTEM_DATABASE_NAME );
        var newestTransactionId = tracker.newestTransactionId( databaseId( SYSTEM_DATABASE_NAME ) );

        assertThat( lastCommittedTxIdAfter ).isGreaterThanOrEqualTo( lastCommittedTxIdBefore + 5 );
        assertEquals( lastCommittedTxIdAfter, newestTransactionId );
    }

    @Test
    void shouldFailToReturnNewestTransactionIdForUnknownDatabase()
    {
        var databaseName = randomDatabaseName();
        dbService.createDatabase( databaseName );
        var unknownDatabaseId = databaseId( databaseName );
        dbService.dropDatabase( databaseName );

        var error = assertThrows( TransactionIdTrackerException.class,
                () -> tracker.newestTransactionId( unknownDatabaseId ) );

        assertEquals( DatabaseNotFound, error.status() );
    }

    @Test
    void shouldFailToReturnNewestTransactionIdForStoppedDatabase()
    {
        var databaseName = randomDatabaseName();
        dbService.createDatabase( databaseName );
        var stoppedDatabaseId = databaseId( databaseName );
        dbService.shutdownDatabase( databaseName );

        var error = assertThrows( TransactionIdTrackerException.class,
                () -> tracker.newestTransactionId( stoppedDatabaseId ) );

        assertEquals( DatabaseUnavailable, error.status() );
    }

    @Test
    void shouldAwaitForTransactionIdOnNonSystemDatabase( TestInfo testInfo )
    {
        var waitTrackingMonitor = new WaitTrackingMonitor();
        monitors.addMonitorListener( waitTrackingMonitor );
        var databaseId = databaseId( nonSystemDb );
        var txIdToWait = lastCommittedTxId( nonSystemDb ) + 100;
        executor = newExecutor( testInfo );

        // wait for a far away transaction ID in a separate thread
        var future = executor.submit( () -> tracker.awaitUpToDate( databaseId, txIdToWait, ofMinutes( 5 ) ) );

        // should be waiting...
        waitTrackingMonitor.clearWaiting();
        assertFalse( future.isDone() );
        assertEventually( "Tracker did not begin waiting", waitTrackingMonitor::isWaiting, TRUE, 30, SECONDS );

        // should still be waiting...
        createNodes( nonSystemDb, 10 );
        waitTrackingMonitor.clearWaiting();
        assertFalse( future.isDone() );
        assertEventually( "Tracker did not continue waiting", waitTrackingMonitor::isWaiting, TRUE, 30, SECONDS );

        // commit enough transactions to release the waiter
        createNodes( nonSystemDb, 100 );

        // should stop waiting
        assertEventually( "Bookmark wait did not complete in time", future::isDone, TRUE, 30, SECONDS );
    }

    @Test
    void shouldAwaitForTransactionIdOnSystemDatabase( TestInfo testInfo )
    {
        var waitTrackingMonitor = new WaitTrackingMonitor();
        monitors.addMonitorListener( waitTrackingMonitor );
        var databaseName = SYSTEM_DATABASE_NAME;
        var txIdToWait = lastCommittedTxId( databaseName ) + 5;
        var databaseId = databaseId( databaseName );
        executor = newExecutor( testInfo );

        // wait for a far away transaction ID in a separate thread
        var future = executor.submit( () -> tracker.awaitUpToDate( databaseId, txIdToWait, ofMinutes( 5 ) ) );

        // should be waiting...
        waitTrackingMonitor.clearWaiting();
        assertFalse( future.isDone() );
        assertEventually( "Tracker did not begin waiting", waitTrackingMonitor::isWaiting, TRUE, 30, SECONDS );

        // should still be waiting...
        createDatabases( 1 );
        waitTrackingMonitor.clearWaiting();
        assertFalse( future.isDone() );
        assertEventually( "Tracker did not continue waiting", waitTrackingMonitor::isWaiting, TRUE, 30, SECONDS );

        // commit enough transactions to release the waiter
        createDatabases( 5 );

        // should stop waiting
        assertEventually( "Bookmark wait did not complete in time", future::isDone, TRUE, 30, SECONDS );
    }

    @Test
    void shouldFailToAwaitForTransactionIdOnUnknownDatabase()
    {
        var databaseName = randomDatabaseName();
        dbService.createDatabase( databaseName );
        var unknownDatabaseId = databaseId( databaseName );
        dbService.dropDatabase( databaseName );

        var error = assertThrows( TransactionIdTrackerException.class,
                () -> tracker.awaitUpToDate( unknownDatabaseId, 1, ofSeconds( 5 ) ) );

        assertEquals( DatabaseNotFound, error.status() );
    }

    @Test
    void shouldFailToAwaitForTransactionIdOnStoppedDatabase()
    {
        var databaseName = randomDatabaseName();
        dbService.createDatabase( databaseName );
        var stoppedDatabaseId = databaseId( databaseName );
        dbService.shutdownDatabase( databaseName );

        var error = assertThrows( TransactionIdTrackerException.class,
                () -> tracker.awaitUpToDate( stoppedDatabaseId, 1, ofSeconds( 5 ) ) );

        assertEquals( DatabaseUnavailable, error.status() );
    }

    @Test
    void shouldFailToAwaitForUnreachableTransactionIdOnNonSystemDatabase()
    {
        var databaseId = databaseId( nonSystemDb );

        var error = assertThrows( TransactionIdTrackerException.class,
                () -> tracker.awaitUpToDate( databaseId, 9999, ofSeconds( 2 ) ) );

        assertEquals( BookmarkTimeout, error.status() );
    }

    @Test
    void shouldFailToAwaitForUnreachableTransactionIdOnSystemDatabase()
    {
        var databaseId = databaseId( SYSTEM_DATABASE_NAME );

        var error = assertThrows( TransactionIdTrackerException.class,
                () -> tracker.awaitUpToDate( databaseId, 9999, ofSeconds( 2 ) ) );

        assertEquals( BookmarkTimeout, error.status() );
    }

    private static void createNodes( GraphDatabaseService db, long count )
    {
        for ( var i = 0; i < count; i++ )
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE ()" ).close();
                transaction.commit();
            }
        }
    }

    private void createDatabases( long count )
    {
        for ( var i = 0; i < count; i++ )
        {
            dbService.createDatabase( randomDatabaseName() );
        }
    }

    private long lastCommittedTxId( String databaseName )
    {
        var db = (GraphDatabaseAPI) dbService.database( databaseName );
        return lastCommittedTxId( db );
    }

    private static long lastCommittedTxId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastCommittedTransactionId();
    }

    private NamedDatabaseId databaseId( String databaseName )
    {
        var db = (GraphDatabaseAPI) dbService.database( databaseName );
        return databaseId( db );
    }

    private static NamedDatabaseId databaseId( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( Database.class ).getNamedDatabaseId();
    }

    private static ExecutorService newExecutor( TestInfo testInfo )
    {
        return Executors.newSingleThreadExecutor( daemon( "thread-" + testInfo.getDisplayName() ) );
    }

    private static String randomDatabaseName()
    {
        return RandomStringUtils.randomAlphabetic( 40 );
    }
}
