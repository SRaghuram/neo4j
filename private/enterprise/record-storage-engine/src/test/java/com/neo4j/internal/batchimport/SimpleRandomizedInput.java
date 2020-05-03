/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntBiFunction;

import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.batchimport.InputIterable;
import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.DataGeneratorInput;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.internal.batchimport.input.InputEntity;
import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.internal.batchimport.input.ReadableGroups;
import org.neo4j.internal.batchimport.input.csv.Header.Entry;
import org.neo4j.internal.batchimport.input.csv.Type;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.values.storable.Value;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SimpleRandomizedInput implements Input
{
    private static final String ID_KEY = "id";

    private final IdType idType = IdType.INTEGER;
    private final Extractors extractors = new Extractors( Configuration.COMMAS.arrayDelimiter() );
    private final Input actual;
    private final long nodeCount;
    private final long relationshipCount;

    SimpleRandomizedInput( long seed, long nodeCount, long relationshipCount,
            float factorBadNodeData, float factorBadRelationshipData )
    {
        this.nodeCount = nodeCount;
        this.relationshipCount = relationshipCount;
        actual = new DataGeneratorInput( nodeCount, relationshipCount, idType, seed, 0,
                DataGeneratorInput.bareboneNodeHeader( ID_KEY, idType, extractors ),
                DataGeneratorInput.bareboneRelationshipHeader( idType, extractors,
                        new Entry( SimpleRandomizedInput.ID_KEY, Type.PROPERTY, null, extractors.int_() ) ),
                4, 4, factorBadNodeData, factorBadRelationshipData );
    }

    @Override
    public InputIterable nodes( Collector badCollector )
    {
        return actual.nodes( badCollector );
    }

    @Override
    public InputIterable relationships( Collector badCollector )
    {
        return actual.relationships( badCollector );
    }

    @Override
    public IdType idType()
    {
        return actual.idType();
    }

    @Override
    public ReadableGroups groups()
    {
        return actual.groups();
    }

    public void verify( GraphDatabaseService db ) throws IOException
    {
        Map<Number,InputEntity> expectedNodeData = new HashMap<>();
        try ( InputIterator nodes = nodes( Collector.EMPTY ).iterator();
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
        try ( InputIterator relationships = relationships( Collector.EMPTY ).iterator();
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
            for ( Relationship relationship : tx.getAllRelationships() )
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
            if ( !expectedRelationshipData.isEmpty() )
            {
                fail( format( "Imported db is missing %d/%d relationships: %s", expectedRelationshipData.size(), relationshipCount,
                        expectedRelationshipData ) );
            }

            long actualNodeCount = 0;
            for ( Node node : tx.getAllNodes() )
            {
                assertNotNull( expectedNodeData.remove( node.getProperty( ID_KEY ) ) );
                actualNodeCount++;
            }
            if ( !expectedNodeData.isEmpty() )
            {
                fail( format( "Imported db is missing %d/%d nodes: %s", expectedNodeData.size(), nodeCount, expectedNodeData ) );
            }
            assertEquals( nodeCount, actualNodeCount );
            assertEquals( relationshipCount, actualRelationshipCount );
            tx.commit();
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
                return other.type == null;
            }
            else
            {
                return type.equals( other.type );
            }
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
    public Estimates calculateEstimates( PropertySizeCalculator valueSizeCalculator ) throws IOException
    {
        return Input.knownEstimates( nodeCount, relationshipCount, 0, 0, 0, 0, 0 );
    }
}
