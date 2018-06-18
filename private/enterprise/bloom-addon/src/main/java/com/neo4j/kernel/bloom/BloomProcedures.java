/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.neo4j.kernel.bloom;

import com.neo4j.kernel.api.impl.fulltext.FulltextAdapter;
import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.EntityType;

import static com.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory.DESCRIPTOR;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;

/**
 * Procedures for querying the bloom fulltext addon.
 */
public class BloomProcedures
{
    public static final String BLOOM_RELATIONSHIPS = "neo4j__internal__bloomRelationships";
    public static final String BLOOM_NODES = "neo4j__internal__bloomNodes";

    @Context
    public KernelTransaction tx;

    @Context
    public FulltextAdapter accessor;

    private static final Function<ScoreEntityIterator.ScoreEntry,EntityOutput> QUERY_RESULT_MAPPER =
            result -> new EntityOutput( result.entityId(), result.score() );

    @Description( "Await the completion of any background index population or updates" )
    @Procedure( name = "bloom.awaitPopulation", mode = READ )
    public void awaitPopulation() throws IndexPopulationFailedKernelException
    {
        await( BLOOM_NODES );
        await( BLOOM_RELATIONSHIPS );
    }

    private void await( String indexName ) throws IndexPopulationFailedKernelException
    {
        //TODO This is here because the core api really doesn't play nicely with multi token schema yet.
        try
        {
            IndexReference index = tx.schemaRead().indexGetForName( indexName );
            InternalIndexState state;
            while ( (state = tx.schemaRead().indexGetState( index )) != InternalIndexState.ONLINE )
            {
                if ( state == InternalIndexState.FAILED )
                {
                    TokenNameLookup lookup = new SilentTokenNameLookup( tx.tokenRead() );
                    throw new IndexPopulationFailedKernelException( index.schema(), index.userDescription( lookup ),
                            "Population of index " + indexName + " has failed." );
                }
            }
        }
        catch ( IndexNotFoundKernelException ignore )
        {
        }
    }

    @Description( "Returns the node property keys indexed by the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.getIndexedNodePropertyKeys", mode = READ )
    public Stream<PropertyOutput> getIndexedNodePropertyKeys()
    {
        return accessor.propertyKeyStrings( tx.schemaRead().indexGetForName( BLOOM_NODES ) ).map( PropertyOutput::new );
    }

    @Description( "Returns the relationship property keys indexed by the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.getIndexedRelationshipPropertyKeys", mode = READ )
    public Stream<PropertyOutput> getIndexedRelationshipPropertyKeys()
    {
        return accessor.propertyKeyStrings( tx.schemaRead().indexGetForName( BLOOM_RELATIONSHIPS ) ).map( PropertyOutput::new );
    }

    @Description( "Set the node property keys to index" )
    @Procedure( name = "bloom.setIndexedNodePropertyKeys", mode = SCHEMA )
    public void setIndexedNodePropertyKeys( @Name( "propertyKeys" ) List<String> propertyKeys ) throws Exception
    {
        tx.schemaWrite().indexDrop( tx.schemaRead().indexGetForName( BLOOM_NODES ) );
        SchemaDescriptor schemaDescriptor = accessor.schemaFor( EntityType.NODE, new String[0], propertyKeys.toArray( new String[0] ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, Optional.of( DESCRIPTOR.name() ), Optional.of( BLOOM_NODES ) );
    }

    @Description( "Set the relationship property keys to index" )
    @Procedure( name = "bloom.setIndexedRelationshipPropertyKeys", mode = SCHEMA )
    public void setIndexedRelationshipPropertyKeys( @Name( "propertyKeys" ) List<String> propertyKeys ) throws Exception
    {
        tx.schemaWrite().indexDrop( tx.schemaRead().indexGetForName( BLOOM_RELATIONSHIPS ) );
        SchemaDescriptor schemaDescriptor = accessor.schemaFor( EntityType.RELATIONSHIP, new String[0], propertyKeys.toArray( new String[0] ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, Optional.of( DESCRIPTOR.name() ), Optional.of( BLOOM_RELATIONSHIPS ) );
    }

    @Description( "Remove the node index" )
    @Procedure( name = "bloom.removeNodeIndex", mode = SCHEMA )
    public void removeNodeIndex() throws Exception
    {
        tx.schemaWrite().indexDrop( tx.schemaRead().indexGetForName( BLOOM_NODES ) );
    }

    @Description( "Remove the relationship index" )
    @Procedure( name = "bloom.removeRelationshipIndex", mode = SCHEMA )
    public void removeRelationshipIndex() throws Exception
    {
        tx.schemaWrite().indexDrop( tx.schemaRead().indexGetForName( BLOOM_RELATIONSHIPS ) );
    }

    @Description( "Check the status of the Bloom fulltext index add-on" )
    @Procedure( name = "bloom.indexStatus", mode = READ )
    public Stream<StatusOutput> indexStatus()
    {
        StatusOutput nodeIndexState = new StatusOutput( BLOOM_NODES, InternalIndexState.FAILED );
        StatusOutput relationshipIndexState = new StatusOutput( BLOOM_RELATIONSHIPS, InternalIndexState.FAILED );
        try
        {
            SchemaRead readOperations = tx.schemaRead();
            InternalIndexState internalNodeIndexState = readOperations.indexGetState( readOperations.indexGetForName( BLOOM_NODES ) );
            InternalIndexState internalRelationshipIndexState = readOperations.indexGetState( readOperations.indexGetForName( BLOOM_RELATIONSHIPS ) );
            nodeIndexState = new StatusOutput( BLOOM_NODES, internalNodeIndexState );
            relationshipIndexState = new StatusOutput( BLOOM_RELATIONSHIPS, internalRelationshipIndexState );
        }
        catch ( IndexNotFoundKernelException ignored )
        {
        }
        return Stream.of( nodeIndexState, relationshipIndexState );
    }

    @Description( "Query the Bloom fulltext index for nodes" )
    @Procedure( name = "bloom.searchNodes", mode = READ )
    public Stream<EntityOutput> bloomFulltextNodes( @Name( "terms" ) List<String> terms, @Name( value = "fuzzy", defaultValue = "true" ) boolean fuzzy,
            @Name( value = "matchAll", defaultValue = "false" ) boolean matchAll ) throws ParseException
    {
        try
        {
            return queryAsStream( terms, BLOOM_NODES, fuzzy, matchAll );
        }
        catch ( IOException | IndexNotFoundKernelException e )
        {
            return Stream.empty();
        }
    }

    @Description( "Query the Bloom fulltext index for relationships" )
    @Procedure( name = "bloom.searchRelationships", mode = READ )
    public Stream<EntityOutput> bloomFulltextRelationships( @Name( "terms" ) List<String> terms, @Name( value = "fuzzy", defaultValue = "true" ) boolean fuzzy,
            @Name( value = "matchAll", defaultValue = "false" ) boolean matchAll ) throws ParseException
    {
        try
        {
            return queryAsStream( terms, BLOOM_RELATIONSHIPS, fuzzy, matchAll );
        }
        catch ( IOException | IndexNotFoundKernelException e )
        {
            return Stream.empty();
        }
    }

    private Stream<EntityOutput> queryAsStream( List<String> terms, String indexName, boolean fuzzy, boolean matchAll )
            throws IOException, IndexNotFoundKernelException, ParseException
    {
        String query = BloomQueryHelper.createQuery( terms, fuzzy, matchAll );
        ScoreEntityIterator resultIterator = accessor.query( indexName, query );
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
