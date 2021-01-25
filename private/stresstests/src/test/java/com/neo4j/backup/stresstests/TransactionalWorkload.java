/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stresstests;

import com.neo4j.causalclustering.stresstests.Control;
import com.neo4j.helper.Workload;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientFailureException;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransactionalWorkload extends Workload
{
    private static final Label LABEL = Label.label( "Label" );
    private static final RelationshipType REL_TYPE = RelationshipType.withName( "KNOWS" );
    private static final String ID_PROPERTY = "id";
    private static final List<String> PROPERTIES = IntStream.range( 0, 8 ).mapToObj( i -> "prop" + i ).collect( toList() );

    private final Supplier<GraphDatabaseService> dbRef;

    private long nodesCreated;

    TransactionalWorkload( Control control, Supplier<GraphDatabaseService> dbRef )
    {
        super( control );
        this.dbRef = dbRef;
    }

    @Override
    public void prepare()
    {
        setupIndexes( dbRef.get() );
    }

    @Override
    protected void doWork()
    {
        GraphDatabaseService db = dbRef.get();
        if ( shouldCreateNodes() )
        {
            createNodes( db );
        }
        else
        {
            updateNodes( db );
        }
    }

    @Override
    public void validate()
    {
        assertThat( "Did not manage to create any data", nodesCreated, greaterThan( 0L ) );
    }

    private boolean shouldCreateNodes()
    {
        if ( nodesCreated == 0 )
        {
            return true;
        }
        // only create new nodes in 20% of the runs to make store files grow less fast
        return ThreadLocalRandom.current().nextInt( 0, 5 ) == 0;
    }

    private void createNodes( GraphDatabaseService db )
    {
        boolean successful = true;
        try ( Transaction tx = db.beginTx() )
        {
            Node node1 = tx.createNode( LABEL );
            Node node2 = tx.createNode( LABEL );

            node1.setProperty( ID_PROPERTY, nodesCreated );
            node2.setProperty( ID_PROPERTY, nodesCreated + 1 );

            for ( String property : PROPERTIES )
            {
                node1.setProperty( property, randomString() );
                node2.setProperty( property, randomString() );
            }
            node1.createRelationshipTo( node2, REL_TYPE );
            tx.commit();
        }
        catch ( DatabaseShutdownException | TransactionFailureException | TransientFailureException ignore )
        {
            successful = false;
        }

        if ( successful )
        {
            nodesCreated += 2;
        }
    }

    private void updateNodes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 10; i++ )
            {
                long randomId = ThreadLocalRandom.current().nextLong( 0, nodesCreated );
                Node node = tx.findNode( LABEL, ID_PROPERTY, randomId );
                assertNotNull( node );
                for ( String property : PROPERTIES )
                {
                    node.setProperty( property, randomString() );
                }
            }
            tx.commit();
        }
        catch ( DatabaseShutdownException | TransactionFailureException | TransientFailureException ignore )
        {
        }
    }

    private static void setupIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().constraintFor( LABEL ).assertPropertyIsUnique( ID_PROPERTY ).create();
            for ( String property : PROPERTIES )
            {
                tx.schema().indexFor( LABEL ).on( property ).create();
            }
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 2, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private static String randomString()
    {
        return UUID.randomUUID().toString();
    }
}
