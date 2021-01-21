/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.index.schema;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.Futures;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.index_population_parallelism;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.index_population_workers;
import static org.neo4j.test.DoubleLatch.awaitLatch;
import static org.neo4j.test.TestLabels.LABEL_ONE;
import static org.neo4j.test.TestLabels.LABEL_THREE;
import static org.neo4j.test.TestLabels.LABEL_TWO;

@TestDirectoryExtension
@ExtendWith( RandomExtension.class )
class EnterpriseIndexPopulationIT
{
    @Inject
    private TestDirectory directory;
    @Inject
    private RandomRule random;

    @Test
    void shouldPopulateMultipleIndexesOnMultipleDbsConcurrentlyWithFewIndexPopulationThreads() throws ExecutionException
    {
        int nbrOfPopulationMainThreads = random.nextInt( 2, 3 );
        int nbrOfPopulationWorkerThreads = random.nextInt( 1, 2 );
        DatabaseManagementService dbms = newDbmsBuilder()
                .setConfig( index_population_parallelism, nbrOfPopulationMainThreads )
                .setConfig( index_population_workers, nbrOfPopulationWorkerThreads )
                .build();
        int nbrOfDbs = 4;
        ExecutorService executorService = newFixedThreadPool( nbrOfDbs );
        CountDownLatch latch = new CountDownLatch( nbrOfPopulationMainThreads );
        Set<Thread> seenThreads = ConcurrentHashMap.newKeySet();
        IndexingService.Monitor populationMonitor = new IndexingService.MonitorAdapter()
        {
            @Override
            public void indexPopulationScanComplete()
            {
                seenThreads.add( Thread.currentThread() );
                latch.countDown();
                awaitLatch( latch );
            }
        };
        try
        {
            List<GraphDatabaseService> dbs = new ArrayList<>();
            for ( int i = 0; i < nbrOfDbs; i++ )
            {
                String dbName = "db" + i;
                dbms.createDatabase( dbName );
                GraphDatabaseAPI db = (GraphDatabaseAPI) dbms.database( dbName );
                db.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener( populationMonitor );
                dbs.add( db );
            }
            createDataOnDbs( executorService, dbs );
            createIndexesOnDbs( executorService, dbs );
            assertThat( seenThreads.size() ).isEqualTo( nbrOfPopulationMainThreads );
        }
        finally
        {
            executorService.shutdown();
            dbms.shutdown();
        }
    }

    private void createIndexesOnDbs( ExecutorService executorService, List<GraphDatabaseService> dbs )
            throws ExecutionException
    {
        List<Future<?>> indexCreate = new ArrayList<>();
        for ( GraphDatabaseService db : dbs )
        {
            indexCreate.add( executorService.submit( () -> {
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( LABEL_ONE ).on( "prop1" ).create();
                    tx.schema().indexFor( LABEL_ONE ).on( "prop2" ).create();
                    tx.schema().indexFor( LABEL_ONE ).on( "prop3" ).create();
                    tx.commit();
                }
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().awaitIndexesOnline( 1, TimeUnit.HOURS );
                    tx.commit();
                }
            } ) );
        }
        Futures.getAll( indexCreate );
    }

    private void createDataOnDbs( ExecutorService executorService, List<GraphDatabaseService> dbs )
            throws ExecutionException
    {
        List<Future<?>> dataCreate = new ArrayList<>();
        for ( GraphDatabaseService db : dbs )
        {
            dataCreate.add( executorService.submit( () -> doCreateData( db ) ) );
        }
        Futures.getAll( dataCreate );
    }

    private void doCreateData( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 100; i++ )
            {
                Node node = tx.createNode( LABEL_ONE, LABEL_TWO, LABEL_THREE );
                node.setProperty( "prop1", "This is a string with a number " + i );
                node.setProperty( "prop2", "This is another string with a number " + i );
                node.setProperty( "prop3", "This is a third string with a number " + i );

            }
            tx.commit();
        }
    }

    private TestEnterpriseDatabaseManagementServiceBuilder newDbmsBuilder()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( directory.homePath() );
    }
}
