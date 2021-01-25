/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cypher.internal.javacompat;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.jupiter.api.Assertions.fail;

@EnterpriseDbmsExtension
public class ParallelRuntimeStressIT
{
    private static final int N_THREADS = 10;
    private static final int ITERATIONS = 10;
    private static final int N_NODES = 100;
    private static final Label LABEL = Label.label( "LABEL" );
    private static final String EXPAND_QUERY = "CYPHER runtime=parallel MATCH (:LABEL)-->(n:LABEL) RETURN n";
    private static final String MATCH_NODE_QUERY = "CYPHER runtime=parallel MATCH (n:LABEL) RETURN n";
    private static final String SYNTAX_ERROR_QUERY = "CYPHER runtime=parallel MATHC (n) RETURN n";
    private static final String RUNTIME_ERROR_QUERY = "CYPHER runtime=parallel MATCH (n) RETURN size($a)";
    private static final Map<String,Object> PARAMS = Map.of( "a", 42 );

    private static final RelationshipType R = RelationshipType.withName( "R" );

    private static final Result.ResultVisitor<RuntimeException> CHECKING_VISITOR = row -> {
        assertThat( row.get( "n" ), notNullValue() );
        return true;
    };
    private static final Result.ResultVisitor<RuntimeException> THROWING_VISITOR = row -> {
        throw new Error( "WHERE IS YOUR GOD NOW" );
    };

    private final AtomicInteger counter = new AtomicInteger( 0 );
    private final ExecutorService service = Executors.newFixedThreadPool( N_THREADS );
    @Inject
    private GraphDatabaseService db;

    @BeforeEach
    void setup()
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node previous = null;
            for ( int i = 0; i < N_NODES; i++ )
            {
                Node node = tx.createNode( LABEL );
                if ( previous != null )
                {
                    previous.createRelationshipTo( node, R );
                }
                previous = node;
            }
            tx.commit();
        }
    }

    @AfterEach
    void tearDown()
    {
        service.shutdownNow();
    }

    @Test
    void runTest() throws InterruptedException
    {
        Task[] tasks = new Task[N_THREADS];
        for ( int i = 0; i < N_THREADS; i++ )
        {
            tasks[i] = new Task();
            service.submit( tasks[i] );
        }
        service.shutdown();
        boolean wasDone = service.awaitTermination( 20, TimeUnit.SECONDS );
        int count = counter.get();
        if ( !wasDone || count != N_THREADS )
        {
            StringBuilder b = new StringBuilder( String.format( "Was done: %b, count: %d, Task iterations finished: ", wasDone, count ) );
            for ( int i = 0; i < N_THREADS; i++ )
            {
                b.append( tasks[i].iterationCount() ).append( ' ' );
            }
            fail( b.toString() );
        }
    }

    private static Result.ResultVisitor<RuntimeException> visitor()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        switch ( random.nextInt( 2 ) )
        {
        case 0:
            return CHECKING_VISITOR;
        case 1:
            return THROWING_VISITOR;
        default:
            throw new IllegalStateException( "this is not a valid state" );
        }
    }

    private static String query()
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        switch ( random.nextInt( 4 ) )
        {
        case 0:
            return EXPAND_QUERY;
        case 1:
            return MATCH_NODE_QUERY;
        case 2:
            return SYNTAX_ERROR_QUERY;
        case 3:
            return RUNTIME_ERROR_QUERY;
        default:
            throw new IllegalStateException( "this is not a valid state" );
        }
    }

    private class Task implements Runnable
    {
        volatile int i;

        @Override
        public void run()
        {
            for ( i = 0; i < ITERATIONS; i++ )
            {
                try
                {
                    db.executeTransactionally( query(), PARAMS, r -> {
                        r.accept( visitor() );
                        return null;
                    } );
                }
                catch ( Throwable t )
                {
                    //ignore
                }
            }
            counter.incrementAndGet();
        }

        int iterationCount()
        {
            return i;
        }
    }
}
