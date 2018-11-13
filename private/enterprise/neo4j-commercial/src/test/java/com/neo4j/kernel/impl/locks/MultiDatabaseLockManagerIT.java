/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locks;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

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
        database = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.databaseDir() );
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
    void databasesHaveDifferentLockManagers()
    {
        GraphDatabaseFacade secondFacade = startSecondDatabase();
        Locks firstDbLocks = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( Locks.class );
        Locks secondDbLocks = secondFacade.getDependencyResolver().resolveDependency( Locks.class );
        assertNotSame( firstDbLocks, secondDbLocks );
    }

    private void acquireLockIn2DifferentDatabases() throws InterruptedException
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

    private GraphDatabaseFacade startSecondDatabase()
    {
        String secondDb = "second.db";
        DatabaseManager databaseManager = getDatabaseManager();
        return databaseManager.createDatabase( secondDb );
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

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

}
