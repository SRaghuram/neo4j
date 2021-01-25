/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locks;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class MultiDatabaseLockManagerIT
{
    private static final long NODE_ID = 1L;

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp()
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    void differentDatabasesCanLockSameEntitiesSimultaneously()
    {
        assertTimeoutPreemptively( Duration.ofMinutes( 1 ), this::acquireLockIn2DifferentDatabases );
    }

    @Test
    void databasesHaveDifferentLockManagers() throws DatabaseExistsException
    {

        GraphDatabaseService secondDatabase = startSecondDatabase();
        Locks firstDbLocks = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( Locks.class );
        Locks secondDbLocks = ((GraphDatabaseAPI) secondDatabase).getDependencyResolver().resolveDependency( Locks.class );
        assertNotSame( firstDbLocks, secondDbLocks );
    }

    private void acquireLockIn2DifferentDatabases() throws InterruptedException, DatabaseExistsException
    {
        ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        try
        {
            GraphDatabaseService secondDatabase = startSecondDatabase();
            CountDownLatch lockAcquiredLatch = new CountDownLatch( 1 );

            try ( Transaction transaction = database.beginTx() )
            {
                Node node = ((InternalTransaction) transaction).newNodeEntity( NODE_ID );
                transaction.acquireWriteLock( node );
                lockNodeWithSameIdInAnotherDatabase( transactionExecutor, secondDatabase, lockAcquiredLatch );
                lockAcquiredLatch.await();
            }
        }
        finally
        {
            transactionExecutor.shutdown();
        }
    }

    private GraphDatabaseService startSecondDatabase() throws DatabaseExistsException
    {
        String secondDb = "second";
        managementService.createDatabase( secondDb );
        return managementService.database( secondDb );
    }

    private static void lockNodeWithSameIdInAnotherDatabase( ExecutorService transactionExecutor, GraphDatabaseService databaseService, CountDownLatch latch )
    {
        transactionExecutor.execute( () -> {
            try ( Transaction transaction = databaseService.beginTx() )
            {
                Node node = ((InternalTransaction) transaction).newNodeEntity( NODE_ID );
                transaction.acquireWriteLock( node );
                latch.countDown();
            }
        } );
    }
}
