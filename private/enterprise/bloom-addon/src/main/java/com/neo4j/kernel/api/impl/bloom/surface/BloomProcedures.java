/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.bloom.surface;

import com.neo4j.kernel.api.impl.bloom.FulltextIndexType;
import com.neo4j.kernel.api.impl.bloom.FulltextProvider;
import com.neo4j.kernel.api.impl.bloom.ReadOnlyFulltext;
import com.neo4j.kernel.api.impl.bloom.ScoreEntityIterator;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;

/**
 * Procedures for querying the bloom fulltext addon.
 */
public class BloomProcedures
{
    @Context
    public FulltextProvider provider;

    private static final Function<ScoreEntityIterator.ScoreEntry,EntityOutput> QUERY_RESULT_MAPPER = result -> new EntityOutput( result.entityId(), result.score() );

    @Description( "Await the completion of any background index population or updates" )
    @Procedure( name = "bloom.awaitPopulation", mode = READ )
    public void awaitPopulation()
    {
        provider.awaitPopulation();
    }

    @Description( "Await index refresh" )
    @Procedure( name = "bloom.awaitRefresh", mode = READ )
    public void awaitRefresh()
    {
        provider.awaitFlip();
    }

    @Description( "Returns the node property keys indexed by the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.getIndexedNodePropertyKeys", mode = READ )
    public Stream<PropertyOutput> getIndexedNodePropertyKeys()
    {
        return provider.getProperties( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES ).stream().map( PropertyOutput::new );
    }

    @Description( "Returns the relationship property keys indexed by the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.getIndexedRelationshipPropertyKeys", mode = READ )
    public Stream<PropertyOutput> getIndexedRelationshipPropertyKeys()
    {
        return provider.getProperties( BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS, FulltextIndexType.RELATIONSHIPS ).stream().map( PropertyOutput::new );
    }

    @Description( "Set the node property keys to index" )
    @Procedure( name = "bloom.setIndexedNodePropertyKeys", mode = SCHEMA )
    public void setIndexedNodePropertyKeys( @Name( "propertyKeys" ) List<String> propertyKeys ) throws Exception
    {
        provider.changeIndexedProperties( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES, propertyKeys );
    }

    @Description( "Set the relationship property keys to index" )
    @Procedure( name = "bloom.setIndexedRelationshipPropertyKeys", mode = SCHEMA )
    public void setIndexedRelationshipPropertyKeys( @Name( "propertyKeys" ) List<String> propertyKeys ) throws Exception
    {
        provider.changeIndexedProperties( BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS, FulltextIndexType.RELATIONSHIPS, propertyKeys );
    }

    @Description( "Check the status of the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.indexStatus", mode = READ )
    public Stream<StatusOutput> indexStatus()
    {
        StatusOutput nodeIndexState = new StatusOutput( BloomKernelExtensionFactory.BLOOM_NODES, provider.getState( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES ) );
        StatusOutput relationshipIndexState = new StatusOutput( BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS, provider.getState( BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS, FulltextIndexType.RELATIONSHIPS ) );
        return Stream.of( nodeIndexState, relationshipIndexState );
    }

    @Description( "Query the Bloom fulltext index for nodes" )
    @Procedure( name = "bloom.searchNodes", mode = READ )
    public Stream<EntityOutput> bloomFulltextNodes(
            @Name( "terms" ) List<String> terms,
            @Name( value = "fuzzy", defaultValue = "true" ) boolean fuzzy,
            @Name( value = "matchAll", defaultValue = "false" ) boolean matchAll ) throws Exception
    {
        try ( ReadOnlyFulltext indexReader = provider.getReader( BloomKernelExtensionFactory.BLOOM_NODES, FulltextIndexType.NODES ) )
        {
            return queryAsStream( terms, indexReader, fuzzy, matchAll );
        }
    }

    @Description( "Query the Bloom fulltext index for relationships" )
    @Procedure( name = "bloom.searchRelationships", mode = READ )
    public Stream<EntityOutput> bloomFulltextRelationships(
            @Name( "terms" ) List<String> terms,
            @Name( value = "fuzzy", defaultValue = "true" ) boolean fuzzy,
            @Name( value = "matchAll", defaultValue = "false" ) boolean matchAll ) throws Exception
    {
        try ( ReadOnlyFulltext indexReader = provider.getReader( BloomKernelExtensionFactory.BLOOM_RELATIONSHIPS, FulltextIndexType.RELATIONSHIPS ) )
        {
            return queryAsStream( terms, indexReader, fuzzy, matchAll );
        }
    }

    private Stream<EntityOutput> queryAsStream( List<String> terms, ReadOnlyFulltext indexReader, boolean fuzzy, boolean matchAll )
    {
        terms = terms.stream().flatMap( s -> Arrays.stream( s.split( "\\s+" ) ) ).collect( Collectors.toList() );
        ScoreEntityIterator resultIterator;
        if ( fuzzy )
        {
            resultIterator = indexReader.fuzzyQuery( terms, matchAll );
        }
        else
        {
            resultIterator = indexReader.query( terms, matchAll );
        }
        return resultIterator.stream().map( QUERY_RESULT_MAPPER );
    }

    public static class EntityOutput
    {
        public final long entityid;
        public final double score;

        EntityOutput( long entityid, float score )
        {
            this.entityid = entityid;
            this.score = score;
        }
    }

    public static class PropertyOutput
    {
        public final String propertyKey;

        PropertyOutput( String propertykey )
        {
            this.propertyKey = propertykey;
        }
    }

    public static class StatusOutput
    {
        public final String name;
        public final String state;

        StatusOutput( String name, InternalIndexState internalIndexState )
        {
            this.name = name;
            switch ( internalIndexState )
            {
            case POPULATING:
                state = "POPULATING";
                break;
            case ONLINE:
                state = "ONLINE";
                break;
            case FAILED:
                state = "FAILED";
                break;
            default:
                throw new IllegalArgumentException( String.format( "Illegal index state %s", internalIndexState ) );
            }
        }
    }
}
