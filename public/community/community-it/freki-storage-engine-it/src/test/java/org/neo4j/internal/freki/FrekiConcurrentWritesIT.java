/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.Race;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith( RandomExtension.class )
@DbmsExtension
class FrekiConcurrentWritesIT
{
    private static final Label LABEL = Label.label( "Dude" );
    private static final String PROPERTY_KEY = "name";
    private static final RandomValues.Configuration CONFIGURATION = new RandomValues.Default()
    {
        @Override
        public int stringMaxLength()
        {
            return 100;
        }

        @Override
        public int arrayMaxLength()
        {
            return 10;
        }
    };

    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RandomRule random;

    /**
     * Used to expose an issue in FrekiTransactionApplier regarding applying big values.
     */
    @Test
    void shouldWriteIndexedArrayValueUpdatesConcurrently()
    {
        // given
        createIndex();
        ConcurrentMap<Long,Value> expectedNames = new ConcurrentHashMap<>();

        // when
        Race race = new Race().withEndCondition( () -> false );
        race.addContestants( 8, i ->
        {
            RandomValues rng = RandomValues.create( new Random( random.seed() + i ), CONFIGURATION );
            return () ->
            {
                float randomValue = rng.nextFloat();
                try ( Transaction tx = database.beginTx() )
                {
                    if ( randomValue < 0.3 )
                    {   // Create node
                        Node node = tx.createNode( LABEL );
                        Value name = setNodeName( rng, node );
                        expectedNames.put( node.getId(), name );
                    }
                    else if ( randomValue < 0.8 )
                    {   // Set node property
                        Node node = randomExistingNode( tx, rng );
                        safeNodeOperation( node, n ->
                        {
                            Value name = setNodeName( rng, n );
                            expectedNames.put( n.getId(), name );
                        } );
                    }
                    else
                    {   // Delete node
                        Node node = randomExistingNode( tx, rng );
                        safeNodeOperation( node, n ->
                        {
                            n.delete();
                            expectedNames.remove( n.getId() );
                        } );
                    }
                    tx.commit();
                }
            };
        }, 500 );
        race.goUnchecked();

        // then
        try ( Transaction tx = database.beginTx() )
        {
            expectedNames.forEach( ( nodeId, nameValue ) ->
            {
                Object name = nameValue.asObject();
                Node nodeToLookFor = tx.getNodeById( nodeId );

                // Check node itself
                assertThat( nodeToLookFor.getProperty( PROPERTY_KEY ) ).isEqualTo( name );

                // Check index
                try ( ResourceIterator<Node> indexedNodes = tx.findNodes( LABEL, PROPERTY_KEY, name ) )
                {
                    boolean found = false;
                    while ( indexedNodes.hasNext() )
                    {
                        if ( indexedNodes.next().equals( nodeToLookFor ) )
                        {
                            found = true;
                            break;
                        }
                    }
                    assertThat( found ).isTrue();
                }
            } );
        }
    }

    private void safeNodeOperation( Node node, Consumer<Node> action )
    {
        if ( node != null )
        {
            try
            {
                action.accept( node );
            }
            catch ( NotFoundException e )
            {
                // OK
            }
        }
    }

    private Value setNodeName( RandomValues rng, Node node )
    {
        Value value = rng.nextArray();
        node.setProperty( PROPERTY_KEY, value.asObject() );
        return value;
    }

    private Node randomExistingNode( Transaction tx, RandomValues rng )
    {
        IdGeneratorFactory idGeneratorFactory = database.getDependencyResolver().resolveDependency( IdGeneratorFactory.class );
        IdGenerator idGenerator = idGeneratorFactory.get( IdType.NODE );
        long highId = idGenerator.getHighId();
        if ( highId == 0 )
        {
            return null;
        }
        try
        {
            return tx.getNodeById( rng.nextLong( highId ) );
        }
        catch ( NotFoundException e )
        {
            return null;
        }
    }

    private void createIndex()
    {
        try ( Transaction tx = database.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROPERTY_KEY ).create();
            tx.commit();
        }
    }
}
