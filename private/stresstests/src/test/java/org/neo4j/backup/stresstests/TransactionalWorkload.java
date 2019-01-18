/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.stresstests;

import com.neo4j.causalclustering.stresstests.Control;

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
import org.neo4j.helper.Workload;

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
            Node node1 = db.createNode( LABEL );
            Node node2 = db.createNode( LABEL );

            node1.setProperty( ID_PROPERTY, nodesCreated );
            node2.setProperty( ID_PROPERTY, nodesCreated + 1 );

            for ( String property : PROPERTIES )
            {
                node1.setProperty( property, randomString() );
                node2.setProperty( property, randomString() );
            }
            node1.createRelationshipTo( node2, REL_TYPE );
            tx.success();
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
                Node node = db.findNode( LABEL, ID_PROPERTY, randomId );
                assertNotNull( node );
                for ( String property : PROPERTIES )
                {
                    node.setProperty( property, randomString() );
                }
            }
            tx.success();
        }
        catch ( DatabaseShutdownException | TransactionFailureException | TransientFailureException ignore )
        {
        }
    }

    private static void setupIndexes( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( LABEL ).assertPropertyIsUnique( ID_PROPERTY ).create();
            for ( String property : PROPERTIES )
            {
                db.schema().indexFor( LABEL ).on( property ).create();
            }
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 1, TimeUnit.MINUTES );
            tx.success();
        }
    }

    private static String randomString()
    {
        return UUID.randomUUID().toString();
    }
}
