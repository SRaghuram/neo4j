/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.restart;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdGenerator;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.input.SimpleDataGenerator;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;

import static org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput.bareboneNodeHeader;
import static org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput.bareboneRelationshipHeader;

public class SimpleRandomizedInput implements Input
{
    static final String ID_KEY = "id";

    private final IdType idType = IdType.INTEGER;
    private final Extractors extractors = new Extractors( Configuration.COMMAS.arrayDelimiter() );
    private final Input actual;
    private final Collector badCollector;
    private final long nodeCount;
    private final long relationshipCount;

    SimpleRandomizedInput( long seed, long nodeCount, long relationshipCount,
            float factorBadNodeData, float factorBadRelationshipData )
    {
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        SimpleDataGenerator generator = new SimpleDataGenerator(
                bareboneNodeHeader( ID_KEY, idType, extractors ),
                bareboneRelationshipHeader( idType, extractors,
                        new Entry( SimpleRandomizedInput.ID_KEY, Type.PROPERTY, null, extractors.int_() ) ),
                seed, nodeCount, 4, 4, idType, factorBadNodeData, factorBadRelationshipData );
        badCollector = new GatheringBadCollector();
        actual = new DataGeneratorInput( nodeCount, relationshipCount,
                generator.nodes(), generator.relationships(), idType, badCollector );
    }

    @Override
    public InputIterable<InputNode> nodes()
    {
        return actual.nodes();
    }

    @Override
    public InputIterable<InputRelationship> relationships()
    {
        return actual.relationships();
    }

    @Override
    public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
    {
        return actual.idMapper( numberArrayFactory );
    }

    @Override
    public IdGenerator idGenerator()
    {
        return actual.idGenerator();
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }

    public void verify( GraphDatabaseService db )
    {
        Map<Number,InputNode> expectedNodeData = new HashMap<>();
        try ( InputIterator<InputNode> nodes = nodes().iterator() )
        {
            Number lastId = null;
            while ( nodes.hasNext() )
            {
                InputNode node = nodes.next();
                Number id = (Number) node.id();
                if ( lastId == null || id.longValue() > lastId.longValue() )
                {
                    expectedNodeData.put( id, node );
                    lastId = id;
                }
            }
        }
        Map<RelationshipKey,Set<InputRelationship>> expectedRelationshipData = new HashMap<>();
        try ( InputIterator<InputRelationship> relationships = relationships().iterator() )
        {
            while ( relationships.hasNext() )
            {
                InputRelationship relationship = relationships.next();
                RelationshipKey key = keyOf( relationship );
                expectedRelationshipData.computeIfAbsent( key, k -> new HashSet<>() ).add( relationship );
            }
        }

        try ( Transaction tx = db.beginTx() )
        {
            long actualRelationshipCount = 0;
            for ( Relationship relationship : db.getAllRelationships() )
            {
                RelationshipKey key = keyOf( relationship );
                Set<InputRelationship> matches = expectedRelationshipData.get( key );
                assertNotNull( matches );
                InputRelationship matchingRelationship = relationshipWithId( matches, relationship );
                assertNotNull( matchingRelationship );
                assertTrue( matches.remove( matchingRelationship ) );
                if ( matches.isEmpty() )
                {
                    expectedRelationshipData.remove( key );
                }
                actualRelationshipCount++;
            }
            assertTrue( expectedRelationshipData.isEmpty() );

            long actualNodeCount = 0;
            for ( Node node : db.getAllNodes() )
            {
                assertTrue( expectedNodeData.remove( node.getProperty( ID_KEY ) ) != null );
                actualNodeCount++;
            }
            assertTrue( expectedNodeData.isEmpty() );
            assertEquals( nodeCount, actualNodeCount );
            assertEquals( relationshipCount, actualRelationshipCount );
            tx.success();
        }
    }

    private static InputRelationship relationshipWithId( Set<InputRelationship> matches, Relationship relationship )
    {
        Map<String,Object> dbProperties = relationship.getAllProperties();
        for ( InputRelationship candidate : matches )
        {
            if ( dbProperties.equals( propertiesOf( candidate ) ) )
            {
                return candidate;
            }
        }
        return null;
    }

    private static Map<String,Object> propertiesOf( InputEntity entity )
    {
        Map<String,Object> result = new HashMap<>();
        Object[] properties = entity.properties();
        for ( int i = 0; i < properties.length; i++ )
        {
            result.put( (String) properties[i++], properties[i] );
        }
        return result;
    }

    private static class RelationshipKey
    {
        final Object startId;
        final String type;
        final Object endId;

        RelationshipKey( Object startId, String type, Object endId )
        {
            this.startId = startId;
            this.type = type;
            this.endId = endId;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((endId == null) ? 0 : endId.hashCode());
            result = prime * result + ((startId == null) ? 0 : startId.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
            {
                return true;
            }
            if ( obj == null || getClass() != obj.getClass() )
            {
                return false;
            }
            RelationshipKey other = (RelationshipKey) obj;
            if ( endId == null )
            {
                if ( other.endId != null )
                {
                    return false;
                }
            }
            else if ( !endId.equals( other.endId ) )
            {
                return false;
            }
            if ( startId == null )
            {
                if ( other.startId != null )
                {
                    return false;
                }
            }
            else if ( !startId.equals( other.startId ) )
            {
                return false;
            }
            if ( type == null )
            {
                if ( other.type != null )
                {
                    return false;
                }
            }
            else if ( !type.equals( other.type ) )
            {
                return false;
            }
            return true;
        }
    }

    private static class GatheringBadCollector implements Collector
    {
        private final Set<RelationshipKey> badRelationships = new HashSet<>();
        private final Set<Object> badNodes = new HashSet<>();

        @Override
        public synchronized void collectBadRelationship( InputRelationship relationship, Object specificValue )
        {
            badRelationships.add( keyOf( relationship ) );
        }

        @Override
        public synchronized void collectDuplicateNode( Object id, long actualId, String group, String firstSource, String otherSource )
        {
            badNodes.add( id );
        }

        @Override
        public void collectExtraColumns( String source, long row, String value )
        {
        }

        @Override
        public long badEntries()
        {
            return badRelationships.size() + badNodes.size();
        }

        @Override
        public PrimitiveLongIterator leftOverDuplicateNodesIds()
        {
            return PrimitiveLongCollections.emptyIterator();
        }

        @Override
        public void close()
        {
        }
    }

    private static RelationshipKey keyOf( InputRelationship relationship )
    {
        return new RelationshipKey( relationship.startNode(), relationship.type(), relationship.endNode() );
    }

    private static RelationshipKey keyOf( Relationship relationship )
    {
        return new RelationshipKey(
                relationship.getStartNode().getProperty( ID_KEY ),
                relationship.getType().name(),
                relationship.getEndNode().getProperty( ID_KEY ) );
    }
}
