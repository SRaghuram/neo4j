/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndex;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexAccessor;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexBuilder;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexReader;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextLuceneIndexPopulator;
import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexProvider;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.storageengine.api.EntityType;

class FulltextIndexProvider extends AbstractLuceneIndexProvider implements FulltextAdapter
{

    private final FileSystemAbstraction fileSystem;
    private final Map<String,FulltextIndexAccessor> accessorsByName;
    private final Config config;
    private final TokenHolders tokenHolders;
    private final OperationalMode operationalMode;
    private final Analyzer analyzer;
    private final String analyzerClassName;

    FulltextIndexProvider( Descriptor descriptor, int priority, IndexDirectoryStructure.Factory directoryStructureFactory, FileSystemAbstraction fileSystem,
            Config config, TokenHolders tokenHolders, DirectoryFactory directoryFactory, OperationalMode operationalMode )
    {
        super( descriptor, priority, directoryStructureFactory, config, operationalMode, fileSystem, directoryFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.tokenHolders = tokenHolders;
        this.operationalMode = operationalMode;

        analyzerClassName = config.get( FulltextConfig.fulltext_default_analyzer );
        this.analyzer = getAnalyzer( analyzerClassName );
        accessorsByName = new HashMap<>();
    }

    @Override
    public InternalIndexState getInitialState( StoreIndexDescriptor descriptor )
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        String failure = indexStorage.getStoredIndexFailure();
        if ( failure != null )
        {
            return InternalIndexState.FAILED;
        }
        //TODO METADAT/ANALYZER
//        if ( !descriptor.analyzer().equals( analyzerClassName ) )
//        {
//            return InternalIndexState.POPULATING;
//        }
        try
        {
            return indexIsOnline( indexStorage, descriptor ) ? InternalIndexState.ONLINE : InternalIndexState.POPULATING;
        }
        catch ( IOException e )
        {
            return InternalIndexState.POPULATING;
        }
    }

    @Override
    public IndexPopulator getPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        FulltextIndexDescriptor fulltextIndexDescriptor = new FulltextIndexDescriptor( descriptor, tokenHolders.propertyKeyTokens(), analyzerClassName );
        FulltextIndex fulltextIndex = FulltextIndexBuilder.create( fulltextIndexDescriptor, config, analyzer ).withFileSystem( fileSystem ).withOperationalMode(
                operationalMode ).withIndexStorage( getIndexStorage( descriptor.getId() ) )
                .withWriterConfig( () -> IndexWriterConfigs.population( analyzer ) )
                .build();
        if ( fulltextIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        return new FulltextLuceneIndexPopulator( fulltextIndexDescriptor, fulltextIndex );
    }

    @Override
    public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        FulltextIndexDescriptor fulltextIndexDescriptor = new FulltextIndexDescriptor( descriptor, tokenHolders.propertyKeyTokens(), analyzerClassName );
        FulltextIndex fulltextIndex = FulltextIndexBuilder.create( fulltextIndexDescriptor, config, analyzer ).withFileSystem( fileSystem ).withOperationalMode(
                operationalMode ).withIndexStorage( getIndexStorage( descriptor.getId() ) ).withWriterConfig(
                () -> IndexWriterConfigs.standard( analyzer ) ).build();
        fulltextIndex.open();

        FulltextIndexAccessor fulltextIndexAccessor = new FulltextIndexAccessor( fulltextIndex, fulltextIndexDescriptor );
        accessorsByName.put( descriptor.getName(), fulltextIndexAccessor );
        return fulltextIndexAccessor;
    }

    @Override
    public Stream<String> propertyKeyStrings( IndexReference index )
    {
        return Arrays.stream( index.schema().getPropertyIds() ).mapToObj( id -> tokenHolders.propertyKeyTokens().getTokenByIdOrNull( id ).name() );
    }

    @Override
    public SchemaDescriptor schemaFor( EntityType type, String[] entityTokens, String... properties )
    {
        if ( Arrays.stream( properties ).anyMatch( prop -> prop.equals( FulltextAdapter.FIELD_ENTITY_ID ) ) )
        {
            throw new BadSchemaException( "Unable to index the property, the name is reserved for internal use " + FulltextAdapter.FIELD_ENTITY_ID );
        }
        int[] entityTokenIds = new int[entityTokens.length];
        if ( type == EntityType.NODE )
        {
            tokenHolders.labelTokens().getOrCreateIds( entityTokens, entityTokenIds );
        }
        else
        {
            tokenHolders.relationshipTypeTokens().getOrCreateIds( entityTokens, entityTokenIds );
        }
        int[] propertyIds = Arrays.stream( properties ).mapToInt( tokenHolders.propertyKeyTokens()::getOrCreateId ).toArray();

        return SchemaDescriptorFactory.multiToken( entityTokenIds, type, propertyIds );
    }

    @Override
    public ScoreEntityIterator query( String indexName, String queryString ) throws IndexNotFoundKernelException, ParseException
    {
        FulltextIndexAccessor fulltextIndexAccessor = accessorsByName.get( indexName );
        if ( fulltextIndexAccessor == null )
        {
            throw new IndexNotFoundKernelException(
                    String.format( "The requested fulltext index with name %s could not be accessed. Perhaps population has not completed yet?", indexName ) );
        }
        try ( FulltextIndexReader fulltextIndexReader = fulltextIndexAccessor.newReader() )
        {
            return fulltextIndexReader.query( queryString );
        }
    }
    private class BadSchemaException extends IllegalArgumentException
    {
        BadSchemaException( String message )
        {
            super( message );
        }
    }

    private Analyzer getAnalyzer( String analyzerClassName )
    {
        Analyzer analyzer;
        try
        {
            Class configuredAnalyzer = Class.forName( analyzerClassName );
            analyzer = (Analyzer) configuredAnalyzer.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create the configured analyzer", e );
        }
        return analyzer;
    }
}
