/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locks;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.dbms.database.DatabaseExistsException;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseLockManagerIT
{
    private static final long NODE_ID = 1L;

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        DatabaseManagementService managementService = new TestCommercialGraphDatabaseFactory().newDatabaseManagementService( testDirectory.storeDir() );
        database = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void differentDatabasesCanLockSameEntitiesSimultaneously()
    {
        assertTimeoutPreemptively( Duration.ofMinutes( 1 ), this::acquireLockIn2DifferentDatabases );
    }

    @Test
    void databasesHaveDifferentLockManagers() throws DatabaseExistsException
    {
        GraphDatabaseFacade secondFacade = startSecondDatabase();
        Locks firstDbLocks = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( Locks.class );
        Locks secondDbLocks = secondFacade.getDependencyResolver().resolveDependency( Locks.class );
        assertNotSame( firstDbLocks, secondDbLocks );
    }

    private void acquireLockIn2DifferentDatabases() throws InterruptedException, DatabaseExistsException
    {
        ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        try
        {
            GraphDatabaseFacade secondFacade = startSecondDatabase();
            CountDownLatch lockAcquiredLatch = new CountDownLatch( 1 );

            GraphDatabaseFacade firstFacade = (GraphDatabaseFacade) database;
            try ( Transaction transaction = firstFacade.beginTx() )
            {
                NodeProxy nodeProxy = firstFacade.newNodeProxy( NODE_ID );
                transaction.acquireWriteLock( nodeProxy );
                lockNodeWithSameIdInAnotherDatabase( transactionExecutor, secondFacade, lockAcquiredLatch );
                lockAcquiredLatch.await();
            }
        }
        finally
        {
            transactionExecutor.shutdown();
        }
    }

    private GraphDatabaseFacade startSecondDatabase() throws DatabaseExistsException
    {
        String secondDb = "second";
        DatabaseManager<?> databaseManager = getDatabaseManager();
        return databaseManager.createDatabase( new DatabaseId( secondDb ) ).databaseFacade();
    }

    private static void lockNodeWithSameIdInAnotherDatabase( ExecutorService transactionExecutor, GraphDatabaseFacade facade, CountDownLatch latch )
    {
        transactionExecutor.execute( () -> {
            try ( Transaction transaction = facade.beginTx() )
            {
                NodeProxy nodeProxy = facade.newNodeProxy( NODE_ID );
                transaction.acquireWriteLock( nodeProxy );
                latch.countDown();
            }
        } );
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

}
