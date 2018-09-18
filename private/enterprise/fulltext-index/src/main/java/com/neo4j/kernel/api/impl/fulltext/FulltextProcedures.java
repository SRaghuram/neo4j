/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.storageengine.api.EntityType;

import static com.neo4j.kernel.api.impl.fulltext.FulltextIndexProviderFactory.DESCRIPTOR;
import static com.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.INDEX_CONFIG_ANALYZER;
import static com.neo4j.kernel.api.impl.fulltext.FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.SCHEMA;

/**
 * Procedures for querying the Fulltext indexes.
 */
@SuppressWarnings( "WeakerAccess" )
public class FulltextProcedures
{
    @Context
    public KernelTransaction tx;

    @Context
    public GraphDatabaseService db;

    @Context
    public FulltextAdapter accessor;

    @Description( "List the available analyzers that the fulltext indexes can be configured with." )
    @Procedure( name = "db.index.fulltext.listAvailableAnalyzers", mode = READ )
    public Stream<AvailableAnalyzer> listAvailableAnalyzers()
    {
        return accessor.listAvailableAnalyzers().map( AvailableAnalyzer::new );
    }

    @Description( "Wait for the updates from recently committed transactions to be applied to any eventually-consistent fulltext indexes." )
    @Procedure( name = "db.index.fulltext.awaitEventuallyConsistentIndexRefresh", mode = READ )
    public void awaitRefresh()
    {
        accessor.awaitRefresh();
    }

    @Description( "Create a node fulltext index for the given labels and properties " +
                  "The optional 'config' map parameter can be used to supply settings to the index. " +
                  "Note: index specific settings are currently experimental, and might not replicated correctly in a cluster, or during backup. " +
                  "Supported settings are '" + INDEX_CONFIG_ANALYZER + "', for specifying what analyzer to use " +
                  "when indexing and querying. Use the `db.index.fulltext.listAvailableAnalyzers` procedure to see what options are available. " +
                  "And '" + INDEX_CONFIG_EVENTUALLY_CONSISTENT + "' which can be set to 'true' to make this index eventually consistent, " +
                  "such that updates from committing transactions are applied in a background thread." )
    @Procedure( name = "db.index.fulltext.createNodeIndex", mode = SCHEMA )
    public void createNodeFulltextIndex(
            @Name( "indexName" ) String name,
            @Name( "labels" ) List<String> labels,
            @Name( "propertyNames" ) List<String> properties,
            @Name( value = "config", defaultValue = "" ) Map<String,String> indexConfigurationMap )
            throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        Properties indexConfiguration = new Properties();
        indexConfiguration.putAll( indexConfigurationMap );
        SchemaDescriptor schemaDescriptor = accessor.schemaFor( EntityType.NODE, stringArray( labels ), indexConfiguration, stringArray( properties ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, DESCRIPTOR.name(), Optional.of( name ) );
    }

    private String[] stringArray( List<String> strings )
    {
        return strings.toArray( ArrayUtils.EMPTY_STRING_ARRAY );
    }

    @Description( "Create a relationship fulltext index for the given relationship types and properties " +
                  "The optional 'config' map parameter can be used to supply settings to the index. " +
                  "Note: index specific settings are currently experimental, and might not replicated correctly in a cluster, or during backup. " +
                  "Supported settings are '" + INDEX_CONFIG_ANALYZER + "', for specifying what analyzer to use " +
                  "when indexing and querying. Use the `db.index.fulltext.listAvailableAnalyzers` procedure to see what options are available. " +
                  "And '" + INDEX_CONFIG_EVENTUALLY_CONSISTENT + "' which can be set to 'true' to make this index eventually consistent, " +
                  "such that updates from committing transactions are applied in a background thread." )
    @Procedure( name = "db.index.fulltext.createRelationshipIndex", mode = SCHEMA )
    public void createRelationshipFulltextIndex(
            @Name( "indexName" ) String name,
            @Name( "relationshipTypes" ) List<String> relTypes,
            @Name( "propertyNames" ) List<String> properties,
            @Name( value = "config", defaultValue = "" ) Map<String,String> config )
            throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        Properties settings = new Properties();
        settings.putAll( config );
        SchemaDescriptor schemaDescriptor = accessor.schemaFor( EntityType.RELATIONSHIP, stringArray( relTypes ), settings, stringArray( properties ) );
        tx.schemaWrite().indexCreate( schemaDescriptor, DESCRIPTOR.name(), Optional.of( name ) );
    }

    @Description( "Drop the specified index." )
    @Procedure( name = "db.index.fulltext.drop", mode = SCHEMA )
    public void drop( @Name( "indexName" ) String name ) throws InvalidTransactionTypeKernelException, SchemaKernelException
    {
        tx.schemaWrite().indexDrop( tx.schemaRead().indexGetForName( name ) );
    }

    @Description( "Query the given fulltext index. Returns the matching nodes and their lucene query score, ordered by score." )
    @Procedure( name = "db.index.fulltext.queryNodes", mode = READ )
    public Stream<NodeOutput> queryFulltextForNodes( @Name( "indexName" ) String name, @Name( "queryString" ) String query )
            throws ParseException, IndexNotFoundKernelException, IOException
    {
        IndexReference indexReference = tx.schemaRead().indexGetForName( name );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.NODE )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for nodes." );
        }
        ScoreEntityIterator resultIterator = accessor.query( tx, name, query );
        return resultIterator.stream()
                .map( result -> NodeOutput.forExistingEntityOrNull( db, result ) )
                .filter( Objects::nonNull );
    }

    @Description( "Query the given fulltext index. Returns the matching relationships and their lucene query score, ordered by score." )
    @Procedure( name = "db.index.fulltext.queryRelationships", mode = READ )
    public Stream<RelationshipOutput> queryFulltextForRelationships( @Name( "indexName" ) String name, @Name( "queryString" ) String query )
            throws ParseException, IndexNotFoundKernelException, IOException
    {
        IndexReference indexReference = tx.schemaRead().indexGetForName( name );
        EntityType entityType = indexReference.schema().entityType();
        if ( entityType != EntityType.RELATIONSHIP )
        {
            throw new IllegalArgumentException( "The '" + name + "' index (" + indexReference + ") is an index on " + entityType +
                    ", so it cannot be queried for relationships." );
        }
        ScoreEntityIterator resultIterator = accessor.query( tx, name, query );
        return resultIterator.stream()
                .map( result -> RelationshipOutput.forExistingEntityOrNull( db, result ) )
                .filter( Objects::nonNull );
    }

    public static final class NodeOutput
    {
        public final Node node;
        public final double score;

        protected NodeOutput( Node node, double score )
        {
            this.node = node;
            this.score = score;
        }

        public static NodeOutput forExistingEntityOrNull( GraphDatabaseService db, ScoreEntityIterator.ScoreEntry result )
        {
            try
            {
                return new NodeOutput( db.getNodeById( result.entityId() ), result.score() );
            }
            catch ( NotFoundException ignore )
            {
                // This node was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    public static final class RelationshipOutput
    {
        public final Relationship relationship;
        public final double score;

        public RelationshipOutput( Relationship relationship, double score )
        {
            this.relationship = relationship;
            this.score = score;
        }

        public static RelationshipOutput forExistingEntityOrNull( GraphDatabaseService db, ScoreEntityIterator.ScoreEntry result )
        {
            try
            {
                return new RelationshipOutput( db.getRelationshipById( result.entityId() ), result.score() );
            }
            catch ( NotFoundException ignore )
            {
                // This relationship was most likely deleted by a concurrent transaction, so we just ignore it.
                return null;
            }
        }
    }

    public static final class AvailableAnalyzer
    {
        public final String analyzer;

        AvailableAnalyzer( String analyzer )
        {
            this.analyzer = analyzer;
        }
    }
}
