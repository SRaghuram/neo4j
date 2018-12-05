/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.javacompat;

import com.neo4j.test.rule.EnterpriseDbmsRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;

public class MorselRuntimeStressIT
{
    private static final int N_THREADS = 10;
    private static final int ITERATIONS = 10;
    private static final int CHUNKS = 100;
    private static final int N_NODES = 100;
    private static final Label LABEL = Label.label( "LABEL" );
    private static final String EXPAND_QUERY = "CYPHER runtime=morsel MATCH (:LABEL)-->(n:LABEL) RETURN n";
    private static final String MATCH_NODE_QUERY = "CYPHER runtime=morsel MATCH (n:LABEL) RETURN n";
    private static final String SYNTAX_ERROR_QUERY = "CYPHER runtime=morsel MATHC (n) RETURN n";
    private static final String RUNTIME_ERROR_QUERY = "CYPHER runtime=morsel MATCH (n) RETURN size($a)";
    private static final Map<String,Object> PARAMS = new HashMap<>();

    static
    {
        PARAMS.put( "a", 42 );
    }

    private static final RelationshipType R = RelationshipType.withName( "R" );

    private static final Result.ResultVisitor<RuntimeException> CHECKING_VISITOR = row -> {
        assertThat( row.get( "n" ), notNullValue() );
        return true;
    };
    private static final Result.ResultVisitor<RuntimeException> THROWING_VISITOR = row -> {
        throw new Error( "WHERE IS YOUR GOD NOW" );
    };

    private AtomicInteger counter = new AtomicInteger( 0 );

    @Rule
    public final EnterpriseDbmsRule db = new EnterpriseDbmsRule();

    private ExecutorService service = Executors.newFixedThreadPool( N_THREADS );
    private Runnable task = () -> {

        for ( int i = 0; i < ITERATIONS; i++ )
        {
            try
            {
                db.execute( query(), PARAMS ).accept( visitor() );
            }
            catch ( Throwable t )
            {
                //ignore
            }
        }
        counter.incrementAndGet();
    };

    @Test
    public void runTest() throws InterruptedException
    {
        for ( int i = 0; i < N_THREADS; i++ )
        {
            service.submit( task );
        }
        service.awaitTermination( 10, TimeUnit.SECONDS );
        assertThat( counter.get(), equalTo( N_THREADS ) );
    }

    private Result.ResultVisitor<RuntimeException> visitor()
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

    private String query()
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

    @Before
    public void setup()
    {
        Transaction tx = null;

        Node previous = null;
        for ( int i = 0; i < N_NODES; i++ )
        {
            if ( i % CHUNKS == 0 )
            {
                if ( tx != null )
                {
                    tx.success();
                    tx.close();
                }
                tx = db.beginTx();
            }
            Node node = db.createNode( LABEL );
            if ( previous != null )
            {
                previous.createRelationshipTo( node, R );
            }
            previous = node;
        }
    }
}
