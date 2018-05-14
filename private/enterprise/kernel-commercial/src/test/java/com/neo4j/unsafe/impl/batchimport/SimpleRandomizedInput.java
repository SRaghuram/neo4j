/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

import org.neo4j.csv.reader.Extractors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.InputChunk;
import org.neo4j.unsafe.impl.batchimport.input.InputEntity;
import org.neo4j.unsafe.impl.batchimport.input.Inputs;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header.Entry;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;
import org.neo4j.unsafe.impl.batchimport.input.csv.Type;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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
        badCollector = new GatheringBadCollector();
        actual = new DataGeneratorInput( nodeCount, relationshipCount, idType, badCollector, seed, 0,
                bareboneNodeHeader( ID_KEY, idType, extractors ),
                bareboneRelationshipHeader( idType, extractors,
                        new Entry( SimpleRandomizedInput.ID_KEY, Type.PROPERTY, null, extractors.int_() ) ),
                4, 4, factorBadNodeData, factorBadRelationshipData );
    }

    @Override
    public InputIterable nodes()
    {
        return actual.nodes();
    }

    @Override
    public InputIterable relationships()
    {
        return actual.relationships();
    }

    @Override
    public IdMapper idMapper( NumberArrayFactory numberArrayFactory )
    {
        return actual.idMapper( numberArrayFactory );
    }

    @Override
    public Collector badCollector()
    {
        return badCollector;
    }

    public void verify( GraphDatabaseService db ) throws IOException
    {
        Map<Number,InputEntity> expectedNodeData = new HashMap<>();
        try ( InputIterator nodes = nodes().iterator();
              InputChunk chunk = nodes.newChunk() )
        {
            Number lastId = null;
            InputEntity node;
            while ( nodes.next( chunk ) )
            {
                while ( chunk.next( node = new InputEntity() ) )
                {
                    Number id = (Number) node.id();
                    if ( lastId == null || id.longValue() > lastId.longValue() )
                    {
                        expectedNodeData.put( id, node );
                        lastId = id;
                    }
                }
            }
        }
        Map<RelationshipKey,Set<InputEntity>> expectedRelationshipData = new HashMap<>();
        try ( InputIterator relationships = relationships().iterator();
              InputChunk chunk = relationships.newChunk() )
        {
            while ( relationships.next( chunk ) )
            {
                InputEntity relationship;
                while ( chunk.next( relationship = new InputEntity() ) )
                {
                    RelationshipKey key = new RelationshipKey( relationship.startId(), relationship.stringType, relationship.endId() );
                    expectedRelationshipData.computeIfAbsent( key, k -> new HashSet<>() ).add( relationship );
                }
            }
        }

        try ( Transaction tx = db.beginTx() )
        {
            long actualRelationshipCount = 0;
            for ( Relationship relationship : db.getAllRelationships() )
            {
                RelationshipKey key = keyOf( relationship );
                Set<InputEntity> matches = expectedRelationshipData.get( key );
                assertNotNull( matches );
                InputEntity matchingRelationship = relationshipWithId( matches, relationship );
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

    private static InputEntity relationshipWithId( Set<InputEntity> matches, Relationship relationship )
    {
        Map<String,Object> dbProperties = relationship.getAllProperties();
        for ( InputEntity candidate : matches )
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
        public synchronized void collectBadRelationship(
                Object startId, String startIdGroup, String type,
                Object endId, String endIdGroup, Object specificValue )
        {
            badRelationships.add( new RelationshipKey( startId, type, endId ) );
        }

        @Override
        public synchronized void collectDuplicateNode( Object id, long actualId, String group )
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
        public void close()
        {
        }

        @Override
        public boolean isCollectingBadRelationships()
        {
            return true;
        }
    }

    private static RelationshipKey keyOf( Relationship relationship )
    {
        return new RelationshipKey(
                relationship.getStartNode().getProperty( ID_KEY ),
                relationship.getType().name(),
                relationship.getEndNode().getProperty( ID_KEY ) );
    }

    @Override
    public Estimates calculateEstimates( ToIntFunction<Value[]> valueSizeCalculator ) throws IOException
    {
        return Inputs.knownEstimates( nodeCount, relationshipCount, 0, 0, 0, 0, 0 );
    }
}
