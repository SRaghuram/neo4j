/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.transaction.stats;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class EnterpriseGlobalTransactionStatsIT
{

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
    void useAggregatedTransactionMonitorForMultidatabase() throws InterruptedException, DatabaseExistsException
    {
        ExecutorService transactionExecutor = Executors.newSingleThreadExecutor();
        String secondDb = "second";
        managementService.createDatabase( secondDb );
        GraphDatabaseFacade secondFacade = (GraphDatabaseFacade) managementService.database( secondDb );

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

            try ( Transaction tx = database.beginTx() )
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
}
