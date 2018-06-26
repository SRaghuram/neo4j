/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
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
public class FulltextProcedures
{
    @Context
    public KernelTransaction tx;

    @Context
    public FulltextAdapter accessor;

    private static final Function<ScoreEntityIterator.ScoreEntry,EntityOutput> QUERY_RESULT_MAPPER =
            result -> new EntityOutput( result.entityId(), result.score() );

    @Description( "Await the completion of any background index population for the given index" )
    @Procedure( name = "fulltext.awaitPopulation", mode = READ )
    public void awaitPopulation( @Name( "indexName" ) String name ) throws IndexPopulationFailedKernelException, IndexNotFoundKernelException
    {
        IndexReference index = tx.schemaRead().indexGetForName( name );
        InternalIndexState state;
        while ( (state = tx.schemaRead().indexGetState( index )) != InternalIndexState.ONLINE )
        {
            if ( state == InternalIndexState.FAILED )
            {
                TokenNameLookup lookup = new SilentTokenNameLookup( tx.tokenRead() );
                throw new IndexPopulationFailedKernelException( index.schema(), index.userDescription( lookup ),
                        "Population of index " + name + " has failed." );
            }
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Description( "Returns the schema for the given index" )
    @Procedure( name = "fulltext.getIndexSchema", mode = READ )
    public Stream<SchemaOutput> getIndexSchema( @Name( "indexName" ) String name )
    {
        TokenNameLookup lookup = new SilentTokenNameLookup( tx.tokenRead() );
        return Stream.of( new SchemaOutput( tx.schemaRead().indexGetForName( name ).schema().userDescription( lookup ) ) );
    }

    @Description( "Create a node fulltext index for all labels and the given properties" )
    @Procedure( name = "fulltext.createAnyNodeLabelIndex", mode = SCHEMA )
    public void createNodeFulltextIndex( @Name( "indexName" ) String name, @Name( "propertyNames" ) List<String> properties )
            throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        createNodeFulltextIndex( name, Collections.emptyList(), properties );
    }

    @Description( "Create a node fulltext index for the given labels and properties" )
    @Procedure( name = "fulltext.createNodeIndex", mode = SCHEMA )
    public void createNodeFulltextIndex( @Name( "indexName" ) String name, @Name( "labels" ) List<String> labels,
            @Name( "propertyNames" ) List<String> properties ) throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        SchemaDescriptor schemaDescriptor = accessor.schemaFor( EntityType.NODE, labels.toArray( new String[0] ), properties.toArray( new String[0] ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, Optional.of( DESCRIPTOR.name() ), Optional.of( name ) );
    }

    @Description( "Create a relationship fulltext index for all relationship types and the given properties" )
    @Procedure( name = "fulltext.createAnyRelationshipTypeIndex", mode = SCHEMA )
    public void createRelationshipFulltextIndex( @Name( "indexName" ) String name, @Name( "propertyNames" ) List<String> properties )
            throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        createRelationshipFulltextIndex( name, Collections.emptyList(), properties );
    }

    @Description( "Create a relationship fulltext index for the given relationship types and properties" )
    @Procedure( name = "fulltext.createRelationshipIndex", mode = SCHEMA )
    public void createRelationshipFulltextIndex( @Name( "indexName" ) String name, @Name( "relatoinshipTypes" ) List<String> reltypes,
            @Name( "propertyNames" ) List<String> properties ) throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        SchemaDescriptor schemaDescriptor =
                accessor.schemaFor( EntityType.RELATIONSHIP, reltypes.toArray( new String[0] ), properties.toArray( new String[0] ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, Optional.of( DESCRIPTOR.name() ), Optional.of( name ) );
    }

    @Description( "Drop the specified index" )
    @Procedure( name = "fulltext.dropIndex", mode = SCHEMA )
    public void dropIndex( @Name( "indexName" ) String name ) throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        tx.schemaWrite().indexDrop( tx.schemaRead().indexGetForName( name ) );
    }

    @Description( "Check the status specified index" )
    @Procedure( name = "fulltext.indexStatus", mode = READ )
    public Stream<StatusOutput> indexStatus( @Name( "indexName" ) String name ) throws IndexNotFoundKernelException
    {
        SchemaRead readOperations = tx.schemaRead();
        InternalIndexState internalNodeIndexState = readOperations.indexGetState( readOperations.indexGetForName( name ) );
        return Stream.of( new StatusOutput( name, internalNodeIndexState ) );
    }

    @Description( "Query the given fulltext index. Returns ids and lucene query score, ordered by score." )
    @Procedure( name = "fulltext.query", mode = READ )
    public Stream<EntityOutput> queryFulltext( @Name( "indexName" ) String name, @Name( "luceneQuery" ) String query )
            throws ParseException, IndexNotFoundKernelException, IOException
    {

        ScoreEntityIterator resultIterator = accessor.query( name, query );
        return resultIterator.stream().map( QUERY_RESULT_MAPPER );
    }

    public static class EntityOutput
    {
        public final long entityId;
        public final double score;

        EntityOutput( long entityId, float score )
        {
            this.entityId = entityId;
            this.score = score;
        }
    }

    public static class SchemaOutput
    {
        public final String schema;

        SchemaOutput( String schema )
        {
            this.schema = schema;
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
