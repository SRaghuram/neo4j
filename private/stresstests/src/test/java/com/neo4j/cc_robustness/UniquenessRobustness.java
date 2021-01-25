/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness;

import com.neo4j.cc_robustness.workload.ReferenceNodeStrategy;
import com.neo4j.cc_robustness.workload.Work;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.internal.helpers.collection.MapUtil;
import org.neo4j.logging.log4j.Log4jLogProvider;

import static org.neo4j.graphdb.Label.label;

public class UniquenessRobustness
{
    private static final String PROPERTY_KEY = "prop1";
    private static final Random random = new Random();
    private static final int NODE_COUNT = 10_000;
    private static final String LABEL = "Label1";

    public static void main( String[] args ) throws Exception
    {
        final Orchestrator orchestrator =
                new Orchestrator( new Log4jLogProvider( System.out ), 3, Orchestrator.JvmMode.same, null, true,
                        ReferenceNodeStrategy.mixed, false, MapUtil.stringMap(), CcRobustness.DEFAULT_ACQUIRE_READ_LOCKS );

        orchestrator.getLeaderInstance().doWorkOnDatabase( (Work) db ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < NODE_COUNT; i++ )
                {
                    tx.createNode( label( LABEL ) );
                }
                tx.commit();
            }
            try ( Transaction tx = db.beginTx() )
            {
                tx.schema().constraintFor( label( LABEL ) ).assertPropertyIsUnique( PROPERTY_KEY ).create();
                tx.commit();
            }
        } );

//        ScheduledExecutorService executor = Executors.newScheduledThreadPool( 8 );
        ExecutorService executor = Executors.newFixedThreadPool( 8 );
        final AtomicInteger successCounter = new AtomicInteger();
        final AtomicInteger collisionCounter = new AtomicInteger();
        final AtomicInteger dropConstraintCounter = new AtomicInteger();
        final AtomicInteger createConstrainCounter = new AtomicInteger();
        final AtomicInteger unableToCreateConstraintCounter = new AtomicInteger();

        Runnable setAndRemoveProperty = new Client( orchestrator, new SetProperty( successCounter, collisionCounter ) );
        Runnable dropAndRecreateConstraint = new Client( orchestrator,
                new DropAndRecreateConstraint( dropConstraintCounter, createConstrainCounter, unableToCreateConstraintCounter ) );

        executor.submit( setAndRemoveProperty );
        executor.submit( setAndRemoveProperty );
        executor.submit( dropAndRecreateConstraint );

        Thread.sleep( 60_000 );

        executor.shutdownNow();
        executor.awaitTermination( 2, TimeUnit.MINUTES );

        System.out.println( "Successes: " + successCounter.get() );
        System.out.println( "Collisions: " + collisionCounter.get() );
        System.out.println( "Dropped Constraints: " + dropConstraintCounter.get() );
        System.out.println( "Created Constraints: " + createConstrainCounter.get() );
        System.out.println( "Constraints that could not be created: " + unableToCreateConstraintCounter.get() );

        orchestrator.shutdownInstances( false );
        orchestrator.ensureInstancesAreShutDown();
    }

    private static class DropAndRecreateConstraint implements Work
    {
        private final AtomicInteger dropConstraintCounter;
        private final AtomicInteger createConstrainCounter;
        private final AtomicInteger unableToCreateConstraintCounter;

        private DropAndRecreateConstraint( AtomicInteger dropConstraintCounter, AtomicInteger createConstrainCounter,
                AtomicInteger unableToCreateConstraintCounter )
        {
            this.dropConstraintCounter = dropConstraintCounter;
            this.createConstrainCounter = createConstrainCounter;
            this.unableToCreateConstraintCounter = unableToCreateConstraintCounter;
        }

        @Override
        public void doWork( GraphDatabaseService db )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Iterable<ConstraintDefinition> constraints = tx.schema().getConstraints( label( LABEL ) );
                for ( ConstraintDefinition constraint : constraints )
                {
                    constraint.drop();
                    dropConstraintCounter.incrementAndGet();
                }
                tx.commit();
            }
            try ( Transaction tx = db.beginTx() )
            {
                try
                {
                    tx.schema().constraintFor( label( LABEL ) ).assertPropertyIsUnique( PROPERTY_KEY ).create();
                    createConstrainCounter.incrementAndGet();
                }
                catch ( ConstraintViolationException e )
                {
                    unableToCreateConstraintCounter.incrementAndGet();
                }
                tx.commit();
            }
        }
    }

    private static class SetProperty implements Work
    {
        private final AtomicInteger setPropertyCounter;
        private final AtomicInteger collisionCounter;

        private SetProperty( AtomicInteger setPropertyCounter, AtomicInteger collisionCounter )
        {
            this.setPropertyCounter = setPropertyCounter;
            this.collisionCounter = collisionCounter;
        }

        private static void verifyConstraint( GraphDatabaseService db, String propertyKey )
        {
            Map<Object,Node> propertyValues = new HashMap<>();
            try ( Transaction tx = db.beginTx() )
            {
                Label label = label( LABEL );
                for ( Node node : tx.getAllNodes() )
                {
                    if ( node.hasLabel( label ) && node.hasProperty( propertyKey ) )
                    {
                        Object propertyValue = node.getProperty( propertyKey );
                        Node previousNode = propertyValues.put( propertyValue, node );
                        if ( previousNode != null )
                        {
                            throw new IllegalStateException(
                                    String.format( "DUPLICATES!!!!! label[%s] propertyKey[%s] propertyValue[%s] nodes[%s] and [%s]", label, propertyKey,
                                            propertyValue, previousNode, node ) );
                        }
                    }
                }
                tx.commit();
            }
        }

        @Override
        public void doWork( GraphDatabaseService db )
        {
            Object value = new boolean[]{true};
            long nodeId = random.nextInt( NODE_COUNT );

            Node node;
            try ( Transaction tx = db.beginTx() )
            {
                node = tx.getNodeById( nodeId );
                try
                {
                    node.setProperty( PROPERTY_KEY, value );
                    setPropertyCounter.incrementAndGet();
                }
                catch ( ConstraintViolationException e )
                {
                    collisionCounter.incrementAndGet();
                    // We expect to get this occasionally.
                    return;
                }
                tx.commit();
            }

//            verifyConstraint( db, PROPERTY_KEY );

            try ( Transaction tx = db.beginTx() )
            {
                node.removeProperty( PROPERTY_KEY );
                tx.commit();
            }
        }
    }

    private static class Client implements Runnable
    {
        private final Orchestrator orchestrator;
        private final Work work;

        Client( Orchestrator orchestrator, Work work )
        {
            this.orchestrator = orchestrator;
            this.work = work;
        }

        @Override
        public void run()
        {
            while ( true )
            {
                try
                {
                    orchestrator.getLeaderInstance().doWorkOnDatabase( work );
                    Thread.sleep( random.nextInt( 500 ) );
                }
                catch ( Exception e )
                {
                    break;
                }
            }
        }
    }
}
