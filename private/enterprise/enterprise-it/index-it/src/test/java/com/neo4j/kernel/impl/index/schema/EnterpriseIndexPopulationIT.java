/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.index.schema;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.index_population_parallelism;
import static org.neo4j.configuration.GraphDatabaseSettings.index_population_workers;
import static org.neo4j.test.TestLabels.LABEL_ONE;
import static org.neo4j.test.TestLabels.LABEL_THREE;
import static org.neo4j.test.TestLabels.LABEL_TWO;

@TestDirectoryExtension
class EnterpriseIndexPopulationIT
{
    @Inject
    private TestDirectory directory;

    @Test
    @Timeout( value = 10, unit = MINUTES )
    void shouldPopulateMultipleIndexesOnMultipleDbsConcurrentlyWithSingleMainThreadAndSingleWorkerThread() throws ExecutionException, InterruptedException
    {
        DatabaseManagementService dbms = newDbmsBuilder()
                .setConfig( index_population_parallelism, 1 )
                .setConfig( index_population_workers, 1 )
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
                jobScheduler.activeGroups()
                        .filter( activeGroup -> Group.INDEX_POPULATION.equals( activeGroup.group ) || Group.INDEX_POPULATION_WORK.equals( activeGroup.group ) )
                        .forEach( activeGroup -> assertEquals( 1, activeGroup.threads ) );
            }
            else
            {
                assertThat( jobScheduler, Matchers.sameInstance( globalJobScheduler ) );
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
        for ( Future<?> future : indexCreate )
        {
            future.get();
        }
    }

    private void createDataOnDbs( ExecutorService executorService, List<GraphDatabaseService> dbs )
            throws InterruptedException, ExecutionException
    {
        List<Future<?>> dataCreate = new ArrayList<>();
        for ( GraphDatabaseService db : dbs )
        {
            dataCreate.add( executorService.submit( () -> doCreateData( db ) ) );
        }
        for ( Future<?> future : dataCreate )
        {
            future.get();
        }
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
        return new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() );
    }
}
