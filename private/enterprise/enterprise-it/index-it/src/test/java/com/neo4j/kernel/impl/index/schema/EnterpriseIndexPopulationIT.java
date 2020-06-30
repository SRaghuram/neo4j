/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.index.schema;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.ActiveGroup;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
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
    private int nbrOfPopulationMainThreads;
    private int nbrOfPopulationWorkerThreads;

    @Test
    void shouldPopulateMultipleIndexesOnMultipleDbsConcurrentlyWithFewIndexPopulationThreads() throws ExecutionException, InterruptedException
    {
        nbrOfPopulationMainThreads = random.nextInt( 1, 2 );
        nbrOfPopulationWorkerThreads = random.nextInt( 1, 2 );
        DatabaseManagementService dbms = newDbmsBuilder()
                .setConfig( index_population_parallelism, nbrOfPopulationMainThreads )
                .setConfig( index_population_workers, nbrOfPopulationWorkerThreads )
                .build();
        int nbrOfDbs = 4;
        ExecutorService executorService = newFixedThreadPool( nbrOfDbs );
        try
        {
            List<GraphDatabaseService> dbs = new ArrayList<>();
            for ( int i = 0; i < nbrOfDbs; i++ )
            {
                String dbName = "db" + i;
                dbms.createDatabase( dbName );
                GraphDatabaseService db = dbms.database( dbName );
                dbs.add( db );
            }
            createDataOnDbs( executorService, dbs );
            createIndexesOnDbs( executorService, dbs );
            assertThreadCount( dbs );
        }
        finally
        {
            executorService.shutdown();
            dbms.shutdown();
        }
    }

    private void assertThreadCount( List<GraphDatabaseService> dbs )
    {
        JobScheduler globalJobScheduler = null;
        for ( GraphDatabaseService db : dbs )
        {
            JobScheduler jobScheduler = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( JobScheduler.class );
            if ( globalJobScheduler == null )
            {
                globalJobScheduler = jobScheduler;
                List<ActiveGroup> activeGroups = jobScheduler.activeGroups().collect( Collectors.toList() );
                for ( ActiveGroup activeGroup : activeGroups )
                {
                    if ( Group.INDEX_POPULATION.equals( activeGroup.group ) )
                    {
                        assertThat( activeGroup.threads ).isLessThanOrEqualTo( nbrOfPopulationMainThreads );
                    }
                    if ( Group.INDEX_POPULATION_WORK.equals( activeGroup.group ) )
                    {
                        assertThat( activeGroup.threads ).isLessThanOrEqualTo( nbrOfPopulationWorkerThreads );
                    }
                }
            }
            else
            {
                assertThat( jobScheduler ).isSameAs( globalJobScheduler );
            }
        }
    }

    private void createIndexesOnDbs( ExecutorService executorService, List<GraphDatabaseService> dbs )
            throws InterruptedException, ExecutionException
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
            throws InterruptedException, ExecutionException
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
