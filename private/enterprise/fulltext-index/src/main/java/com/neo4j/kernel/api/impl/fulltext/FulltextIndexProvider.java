/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexAccessor;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexBuilder;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexReader;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexPopulator;
import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexProvider;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
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
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexReader;

class FulltextIndexProvider extends AbstractLuceneIndexProvider implements FulltextAdapter
{

    private final FileSystemAbstraction fileSystem;
    private final Config config;
    private final Supplier<TokenHolders> tokenHolders;
    private final OperationalMode operationalMode;
    private final String defaultAnalyzerClassName;

    FulltextIndexProvider( Descriptor descriptor, int priority, IndexDirectoryStructure.Factory directoryStructureFactory, FileSystemAbstraction fileSystem,
            Config config, Supplier<TokenHolders> tokenHolders, DirectoryFactory directoryFactory, OperationalMode operationalMode )
    {
        super( descriptor, priority, directoryStructureFactory, config, operationalMode, fileSystem, directoryFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.tokenHolders = tokenHolders;
        this.operationalMode = operationalMode;

        defaultAnalyzerClassName = config.get( FulltextConfig.fulltext_default_analyzer );
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
        TokenHolders tokens = tokenHolders.get();
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        FulltextIndexDescriptor fulltextIndexDescriptor = FulltextIndexSettings.readOrInitialiseDescriptor(
                descriptor, defaultAnalyzerClassName, tokens.propertyKeyTokens(), indexStorage, fileSystem );
        DatabaseIndex<FulltextIndexReader> fulltextIndex = FulltextIndexBuilder
                .create( fulltextIndexDescriptor, config )
                .withFileSystem( fileSystem )
                .withOperationalMode( operationalMode )
                .withIndexStorage( indexStorage )
                .withPopulatingMode( true )
                .build();
        if ( fulltextIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        return new FulltextIndexPopulator( fulltextIndexDescriptor, fulltextIndex,
                () -> FulltextIndexSettings.saveFulltextIndexSettings( fulltextIndexDescriptor, indexStorage, fileSystem ) );
    }

    @Override
    public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        TokenHolders tokens = tokenHolders.get();
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );

        FulltextIndexDescriptor fulltextIndexDescriptor = FulltextIndexSettings.readOrInitialiseDescriptor(
                descriptor, defaultAnalyzerClassName, tokens.propertyKeyTokens(), indexStorage, fileSystem );
        DatabaseIndex<FulltextIndexReader> fulltextIndex = FulltextIndexBuilder
                .create( fulltextIndexDescriptor, config )
                .withFileSystem( fileSystem )
                .withOperationalMode( operationalMode )
                .withIndexStorage( indexStorage )
                .withPopulatingMode( false )
                .build();
        fulltextIndex.open();

        return new FulltextIndexAccessor( fulltextIndex, fulltextIndexDescriptor );
    }

    @Override
    public SchemaDescriptor schemaFor( EntityType type, String[] entityTokens, Properties settings, String... properties )
    {
        if ( entityTokens.length == 0 )
        {
            throw new BadSchemaException(
                    "At least one " + ( type == EntityType.NODE ? "label" : "relationship type" ) + " must be specified when creating a fulltext index." );
        }
        if ( properties.length == 0 )
        {
            throw new BadSchemaException( "At least one property name must be specified when creating a fulltext index." );
        }
        TokenHolders tokens = tokenHolders.get();
        if ( Arrays.stream( properties ).anyMatch( prop -> prop.equals( FulltextAdapter.FIELD_ENTITY_ID ) ) )
        {
            throw new BadSchemaException( "Unable to index the property, the name is reserved for internal use " + FulltextAdapter.FIELD_ENTITY_ID );
        }
        int[] entityTokenIds = new int[entityTokens.length];
        if ( type == EntityType.NODE )
        {
            tokens.labelTokens().getOrCreateIds( entityTokens, entityTokenIds );
        }
        else
        {
            tokens.relationshipTypeTokens().getOrCreateIds( entityTokens, entityTokenIds );
        }
        int[] propertyIds = Arrays.stream( properties ).mapToInt( tokens.propertyKeyTokens()::getOrCreateId ).toArray();

        SchemaDescriptor schema = SchemaDescriptorFactory.multiToken( entityTokenIds, type, propertyIds );
        if ( !settings.containsKey( FulltextIndexSettings.SETTING_ANALYZER ) )
        {
            settings.put( FulltextIndexSettings.SETTING_ANALYZER, defaultAnalyzerClassName );
        }
        return new FulltextSchemaDescriptor( schema, settings );
    }

    @Override
    public ScoreEntityIterator query( KernelTransaction ktx, String indexName, String queryString ) throws IndexNotFoundKernelException, ParseException
    {
        AllStoreHolder allStoreHolder = (AllStoreHolder) ktx.dataRead();
        IndexReference indexReference = ktx.schemaRead().indexGetForName( indexName );
        IndexReader indexReader = allStoreHolder.indexReader( indexReference, false );
        FulltextIndexReader fulltextIndexReader = (FulltextIndexReader) indexReader;
        return fulltextIndexReader.query( queryString );
    }

    private class BadSchemaException extends IllegalArgumentException
    {
        BadSchemaException( String message )
        {
            super( message );
        }
    }
}
