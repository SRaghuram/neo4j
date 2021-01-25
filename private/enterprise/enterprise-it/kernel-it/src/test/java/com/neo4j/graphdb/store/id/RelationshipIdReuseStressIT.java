/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.graphdb.store.id;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.util.concurrent.Futures;

import static java.lang.System.currentTimeMillis;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@EnterpriseDbmsExtension
class RelationshipIdReuseStressIT
{
    @Inject
    private GraphDatabaseAPI embeddedDatabase;
    @Inject
    private IdController idController;
    @Inject
    private IdGeneratorFactory idGeneratorFactory;

    private final ExecutorService executorService = Executors.newCachedThreadPool();

    private final String NAME_PROPERTY = "name";
    private static final int NUMBER_OF_BANDS = 3;
    private static final int NUMBER_OF_CITIES = 10;

    @AfterAll
    void tearDown()
    {
        executorService.shutdown();
    }

    @Test
    void relationshipIdReused() throws Exception
    {
        Label cityLabel = Label.label( "city" );
        final Label bandLabel = Label.label( "band" );
        createBands( bandLabel );
        createCities( cityLabel );

        AtomicBoolean stopFlag = new AtomicBoolean( false );
        RelationshipsCreator relationshipsCreator = new RelationshipsCreator( stopFlag, bandLabel, cityLabel );
        RelationshipRemover relationshipRemover = new RelationshipRemover( bandLabel, cityLabel, stopFlag );

        assertNotNull( idController, "idController was null for some reason" );

        List<Future<?>> futures = new ArrayList<>();
        futures.add( executorService.submit( relationshipRemover ) );
        futures.add( executorService.submit( relationshipsCreator ) );
        futures.add( startRelationshipTypesCalculator( bandLabel, stopFlag ) );
        futures.add( startRelationshipCalculator( bandLabel, stopFlag ) );

        long startTime = currentTimeMillis();
        long currentTime;
        long createdRelationships;
        long removedRelationships;
        do
        {
            TimeUnit.MILLISECONDS.sleep( 500 );
            idController.maintenance( true ); // just to make sure maintenance happens
            currentTime = currentTimeMillis();
            createdRelationships = relationshipsCreator.getCreatedRelationships();
            removedRelationships = relationshipRemover.getRemovedRelationships();
        }
        while ( (currentTime - startTime) < 5_000 || createdRelationships < 1_000 || removedRelationships < 100 );
        stopFlag.set( true );
        executorService.shutdown();
        Futures.getAll( futures );

        long highestPossibleIdInUse = getHighestUsedIdForRelationships();
        assertThat( relationshipsCreator.getCreatedRelationships() ).as(
                "Number of created relationships should be higher then highest possible id, since those are " + "reused." ).isGreaterThan(
                highestPossibleIdInUse );
    }

    private long getHighestUsedIdForRelationships()
    {
        return idGeneratorFactory.get( IdType.RELATIONSHIP ).getHighestPossibleIdInUse();
    }

    private void createCities( Label cityLabel )
    {
        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            for ( int i = 1; i <= NUMBER_OF_CITIES; i++ )
            {
                createLabeledNamedNode( transaction, cityLabel, "city" + i );
            }
            transaction.commit();
        }
    }

    private void createBands( Label bandLabel )
    {
        try ( Transaction transaction = embeddedDatabase.beginTx() )
        {
            for ( int i = 1; i <= NUMBER_OF_BANDS; i++ )
            {
                createLabeledNamedNode( transaction, bandLabel, "band" + i );
            }
            transaction.commit();
        }
    }

    private Future<?> startRelationshipCalculator( final Label bandLabel, final AtomicBoolean stopFlag )
    {
        return executorService.submit( new RelationshipCalculator( stopFlag, bandLabel ) );
    }

    private Future<?> startRelationshipTypesCalculator( final Label bandLabel, final AtomicBoolean stopFlag )
    {
        return executorService.submit( new RelationshipTypeCalculator( stopFlag, bandLabel ) );
    }

    private static Direction getRandomDirection()
    {
        return Direction.values()[ThreadLocalRandom.current().nextInt( Direction.values().length )];
    }

    private static TestRelationshipTypes getRandomRelationshipType()
    {
        return TestRelationshipTypes.values()[ThreadLocalRandom.current().nextInt( TestRelationshipTypes.values().length )];
    }

    private Node getRandomCityNode( Transaction transaction, Label cityLabel )
    {
        return transaction.
                findNode( cityLabel, NAME_PROPERTY, "city" + (ThreadLocalRandom.current().nextInt( 1, NUMBER_OF_CITIES + 1 )) );
    }

    private Node getRandomBandNode( Transaction transaction, Label bandLabel )
    {
        return transaction.
                findNode( bandLabel, NAME_PROPERTY, "band" + (ThreadLocalRandom.current().nextInt( 1, NUMBER_OF_BANDS + 1 )) );
    }

    private void createLabeledNamedNode( Transaction tx, Label label, String name )
    {
        Node node = tx.createNode( label );
        node.setProperty( NAME_PROPERTY, name );
    }

    private enum TestRelationshipTypes implements RelationshipType
    {
        LIKE,
        HATE,
        NEUTRAL
    }

    private class RelationshipsCreator implements Runnable
    {
        private final AtomicBoolean stopFlag;
        private final Label bandLabel;
        private final Label cityLabel;

        private volatile long createdRelationships;

        RelationshipsCreator( AtomicBoolean stopFlag, Label bandLabel, Label cityLabel )
        {
            this.stopFlag = stopFlag;
            this.bandLabel = bandLabel;
            this.cityLabel = cityLabel;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                int newRelationships = 0;
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    Node bandNode = getRandomBandNode( transaction, bandLabel );
                    int direction = ThreadLocalRandom.current().nextInt( 3 );
                    switch ( direction )
                    {
                    case 0:
                        newRelationships += connectCitiesToBand( transaction, bandNode );
                        break;
                    case 1:
                        newRelationships += connectBandToCities( transaction, bandNode );
                        break;
                    case 2:
                        newRelationships += connectCitiesToBand( transaction, bandNode );
                        newRelationships += connectBandToCities( transaction, bandNode );
                        break;
                    default:
                        throw new IllegalStateException( "Unsupported direction value:" + direction );
                    }
                    transaction.commit();
                }
                catch ( DeadlockDetectedException ignored )
                {
                    // deadlocks ignored
                }
                createdRelationships += newRelationships;
                long millisToWait = ThreadLocalRandom.current().nextLong( 10, 30 );
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( millisToWait ) );
            }
        }

        private int connectBandToCities( Transaction transaction, Node bandNode )
        {
            Node city1 = getRandomCityNode( transaction, cityLabel );
            Node city2 = getRandomCityNode( transaction, cityLabel );
            Node city3 = getRandomCityNode( transaction, cityLabel );
            Node city4 = getRandomCityNode( transaction, cityLabel );
            Node city5 = getRandomCityNode( transaction, cityLabel );

            bandNode.createRelationshipTo( city1, TestRelationshipTypes.LIKE );
            bandNode.createRelationshipTo( city2, TestRelationshipTypes.LIKE );
            bandNode.createRelationshipTo( city3, TestRelationshipTypes.HATE );
            bandNode.createRelationshipTo( city4, TestRelationshipTypes.LIKE );
            bandNode.createRelationshipTo( city5, TestRelationshipTypes.NEUTRAL );
            return 5;
        }

        private int connectCitiesToBand( Transaction transaction, Node bandNode )
        {
            Node city1 = getRandomCityNode( transaction, cityLabel );
            Node city2 = getRandomCityNode( transaction, cityLabel );
            Node city3 = getRandomCityNode( transaction, cityLabel );
            Node city4 = getRandomCityNode( transaction, cityLabel );
            city1.createRelationshipTo( bandNode, TestRelationshipTypes.LIKE );
            city2.createRelationshipTo( bandNode, TestRelationshipTypes.HATE );
            city3.createRelationshipTo( bandNode, TestRelationshipTypes.LIKE );
            city4.createRelationshipTo( bandNode, TestRelationshipTypes.NEUTRAL );
            return 4;
        }

        long getCreatedRelationships()
        {
            return createdRelationships;
        }
    }

    private class RelationshipCalculator implements Runnable
    {
        private final AtomicBoolean stopFlag;
        private final Label bandLabel;
        private int relationshipSize;

        RelationshipCalculator( AtomicBoolean stopFlag, Label bandLabel )
        {
            this.stopFlag = stopFlag;
            this.bandLabel = bandLabel;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    Node randomBandNode = getRandomBandNode( transaction, bandLabel );
                    relationshipSize = Iterables.asList( randomBandNode.getRelationships() ).size();
                    transaction.commit();
                }
                long millisToWait = ThreadLocalRandom.current().nextLong( 10, 25 );
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( millisToWait ) );
            }
        }
    }

    private class RelationshipTypeCalculator implements Runnable
    {
        private final AtomicBoolean stopFlag;
        private final Label bandLabel;
        private int relationshipSize;

        RelationshipTypeCalculator( AtomicBoolean stopFlag, Label bandLabel )
        {
            this.stopFlag = stopFlag;
            this.bandLabel = bandLabel;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    Node randomBandNode = getRandomBandNode( transaction, bandLabel );
                    relationshipSize = Iterables.asList( randomBandNode.getRelationshipTypes()).size();
                    transaction.commit();
                }
                long millisToWait = ThreadLocalRandom.current().nextLong( 10, 25 );
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( millisToWait ) );
            }
        }
    }

    private class RelationshipRemover implements Runnable
    {
        private final Label bandLabel;
        private final Label cityLabel;
        private final AtomicBoolean stopFlag;

        private volatile int removalCount;

        RelationshipRemover( Label bandLabel, Label cityLabel, AtomicBoolean stopFlag )
        {
            this.bandLabel = bandLabel;
            this.cityLabel = cityLabel;
            this.stopFlag = stopFlag;
        }

        @Override
        public void run()
        {
            while ( !stopFlag.get() )
            {
                try ( Transaction transaction = embeddedDatabase.beginTx() )
                {
                    boolean deleteOnBands = ThreadLocalRandom.current().nextBoolean();
                    if ( deleteOnBands )
                    {
                        deleteRelationshipOfRandomType( transaction );
                    }
                    else
                    {
                        deleteRelationshipOnRandomNode( transaction );

                    }
                    transaction.commit();
                    removalCount++;
                }
                catch ( DeadlockDetectedException | NotFoundException ignored )
                {
                    // ignore deadlocks
                }
                LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 15 ) );
            }
        }

        int getRemovedRelationships()
        {
            return removalCount;
        }

        private void deleteRelationshipOfRandomType( Transaction transaction )
        {
            Node bandNode = getRandomBandNode( transaction, bandLabel );
            TestRelationshipTypes relationshipType = getRandomRelationshipType();
            Iterable<Relationship> relationships = bandNode.getRelationships( getRandomDirection(), relationshipType );
            for ( Relationship relationship : relationships )
            {
                relationship.delete();
            }
        }

        private void deleteRelationshipOnRandomNode( Transaction transaction )
        {
            try ( ResourceIterator<Node> nodeResourceIterator = transaction.findNodes( cityLabel ) )
            {
                List<Node> nodes = Iterators.asList( nodeResourceIterator );
                int index = ThreadLocalRandom.current().nextInt( nodes.size() );
                Node node = nodes.get( index );
                for ( Relationship relationship : node.getRelationships() )
                {
                    relationship.delete();
                }
            }
        }
    }
}
