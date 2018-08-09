/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.transaction.stats;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( TestDirectoryExtension.class )
class GlobalTransactionStatsIT
{

    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;

    @BeforeEach
    void setUp()
    {
        database = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.storeDir() );
    }

    @AfterEach
    void tearDown()
    {
        database.shutdown();
    }

    @Test
    void useAggregatedTransactionMonitorForMultidatabase() throws InterruptedException
    {
        ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        String secondDb = "second.db";
        DatabaseManager databaseManager = getDatabaseManager();
        GraphDatabaseFacade secondFacade = databaseManager.createDatabase( secondDb );

        GlobalTransactionStats globalTransactionStats = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( GlobalTransactionStats.class );
        assertEquals( 0, globalTransactionStats.getNumberOfActiveTransactions() );
        CountDownLatch startSeparateTransaction = new CountDownLatch( 1 );
        try
        {
            transactionExecutor.execute( () ->
            {
                secondFacade.beginTx();
                startSeparateTransaction.countDown();
            } );
            startSeparateTransaction.await();
            assertEquals( 1, globalTransactionStats.getNumberOfActiveTransactions() );

            try ( Transaction ignored = database.beginTx() )
            {
                TransactionCounters databaseStats = ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( TransactionCounters.class );
                assertEquals( 2, globalTransactionStats.getNumberOfActiveTransactions() );
                assertEquals( 1, databaseStats.getNumberOfActiveTransactions() );
            }
        }
        finally
        {
            transactionExecutor.shutdown();
        }
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
