/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.queryparser.classic.ParseException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.index.fulltext.AnalyzerProvider;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndexProvider;
import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionState;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

class FulltextIndexProvider extends AbstractLuceneIndexProvider implements FulltextAdapter, AuxiliaryTransactionStateProvider
{
    private static final String TX_STATE_PROVIDER_KEY = "FULLTEXT SCHEMA INDEX TRANSACTION STATE";

    private final FileSystemAbstraction fileSystem;
    private final Config config;
    private final TokenHolders tokenHolders;
    private final OperationalMode operationalMode;
    private final String defaultAnalyzerName;
    private final String defaultEventuallyConsistentSetting;
    private final AuxiliaryTransactionStateManager auxiliaryTransactionStateManager;
    private final Log log;
    private final IndexUpdateSink indexUpdateSink;
    private final ConcurrentMap<StoreIndexDescriptor,FulltextIndexAccessor> openOnlineAccessors;

    FulltextIndexProvider( IndexProviderDescriptor descriptor, IndexDirectoryStructure.Factory directoryStructureFactory,
            FileSystemAbstraction fileSystem, Config config, TokenHolders tokenHolders, DirectoryFactory directoryFactory, OperationalMode operationalMode,
            JobScheduler scheduler, AuxiliaryTransactionStateManager auxiliaryTransactionStateManager, Log log )
    {
        super( descriptor, directoryStructureFactory, config, operationalMode, fileSystem, directoryFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.tokenHolders = tokenHolders;
        this.operationalMode = operationalMode;
        this.auxiliaryTransactionStateManager = auxiliaryTransactionStateManager;
        this.log = log;

        defaultAnalyzerName = config.get( FulltextConfig.fulltext_default_analyzer );
        defaultEventuallyConsistentSetting = Boolean.toString( config.get( FulltextConfig.eventually_consistent ) );
        indexUpdateSink = new IndexUpdateSink( scheduler, config.get( FulltextConfig.eventually_consistent_index_update_queue_max_length ) );
        openOnlineAccessors = new ConcurrentHashMap<>();
    }

    @Override
    public void start() throws Throwable
    {
        super.start();
        auxiliaryTransactionStateManager.registerProvider( this );
    }

    @Override
    public void stop() throws Throwable
    {
        auxiliaryTransactionStateManager.unregisterProvider( this );
        super.stop();
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
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );
        FulltextIndexDescriptor fulltextIndexDescriptor = FulltextIndexSettings.readOrInitialiseDescriptor(
                descriptor, defaultAnalyzerName, tokenHolders.propertyKeyTokens(), indexStorage, fileSystem );
        DatabaseIndex<FulltextIndexReader> fulltextIndex = FulltextIndexBuilder
                .create( fulltextIndexDescriptor, config, tokenHolders.propertyKeyTokens() )
                .withFileSystem( fileSystem )
                .withOperationalMode( operationalMode )
                .withIndexStorage( indexStorage )
                .withPopulatingMode( true )
                .build();
        if ( fulltextIndex.isReadOnly() )
        {
            throw new UnsupportedOperationException( "Can't create populator for read only index" );
        }
        log.debug( "Creating populator for fulltext schema index: %s", descriptor );
        return new FulltextIndexPopulator( fulltextIndexDescriptor, fulltextIndex,
                () -> FulltextIndexSettings.saveFulltextIndexSettings( fulltextIndexDescriptor, indexStorage, fileSystem ) );
    }

    @Override
    public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        PartitionedIndexStorage indexStorage = getIndexStorage( descriptor.getId() );

        FulltextIndexDescriptor fulltextIndexDescriptor = FulltextIndexSettings.readOrInitialiseDescriptor(
                descriptor, defaultAnalyzerName, tokenHolders.propertyKeyTokens(), indexStorage, fileSystem );
        FulltextIndexBuilder fulltextIndexBuilder = FulltextIndexBuilder
                .create( fulltextIndexDescriptor, config, tokenHolders.propertyKeyTokens() )
                .withFileSystem( fileSystem )
                .withOperationalMode( operationalMode )
                .withIndexStorage( indexStorage )
                .withPopulatingMode( false );
        if ( fulltextIndexDescriptor.isEventuallyConsistent() )
        {
            fulltextIndexBuilder = fulltextIndexBuilder.withIndexUpdateSink( indexUpdateSink );
        }
        DatabaseFulltextIndex fulltextIndex = fulltextIndexBuilder.build();
        fulltextIndex.open();

        Runnable onClose = () -> openOnlineAccessors.remove( descriptor );
        FulltextIndexAccessor accessor = new FulltextIndexAccessor( indexUpdateSink, fulltextIndex, fulltextIndexDescriptor, onClose );
        openOnlineAccessors.put( descriptor, accessor );
        log.debug( "Created online accessor for fulltext schema index %s: %s", descriptor, accessor );
        return accessor;
    }

    FulltextIndexAccessor getOpenOnlineAccessor( StoreIndexDescriptor descriptor )
    {
        return openOnlineAccessors.get( descriptor );
    }

    @Override
    public SchemaDescriptor schemaFor( EntityType type, String[] entityTokens, Properties indexConfiguration, String... properties )
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
        if ( Arrays.asList( properties ).contains( LuceneFulltextDocumentStructure.FIELD_ENTITY_ID ) )
        {
            throw new BadSchemaException( "Unable to index the property, the name is reserved for internal use " +
                    LuceneFulltextDocumentStructure.FIELD_ENTITY_ID );
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

        SchemaDescriptor schema = SchemaDescriptorFactory.multiToken( entityTokenIds, type, propertyIds );
        indexConfiguration.putIfAbsent( FulltextIndexSettings.INDEX_CONFIG_ANALYZER, defaultAnalyzerName );
        indexConfiguration.putIfAbsent( FulltextIndexSettings.INDEX_CONFIG_EVENTUALLY_CONSISTENT, defaultEventuallyConsistentSetting );
        return new FulltextSchemaDescriptor( schema, indexConfiguration );
    }

    @Override
    public ScoreEntityIterator query( KernelTransaction ktx, String indexName, String queryString ) throws IndexNotFoundKernelException, ParseException
    {
        KernelTransactionImplementation kti = (KernelTransactionImplementation) ktx;
        AllStoreHolder allStoreHolder = (AllStoreHolder) kti.dataRead();
        IndexReference indexReference = kti.schemaRead().indexGetForName( indexName );
        FulltextIndexReader fulltextIndexReader;
        if ( kti.hasTxStateWithChanges() && !((FulltextSchemaDescriptor) indexReference.schema()).isEventuallyConsistent() )
        {
            FulltextAuxiliaryTransactionState auxiliaryTxState = (FulltextAuxiliaryTransactionState) allStoreHolder.auxiliaryTxState( TX_STATE_PROVIDER_KEY );
            fulltextIndexReader = auxiliaryTxState.indexReader( indexReference, kti );
        }
        else
        {
            IndexReader indexReader = allStoreHolder.indexReader( indexReference, false );
            fulltextIndexReader = (FulltextIndexReader) indexReader;
        }
        return fulltextIndexReader.query( queryString );
    }

    @Override
    public void awaitRefresh()
    {
        indexUpdateSink.awaitUpdateApplication();
    }

    @Override
    public Stream<String> listAvailableAnalyzers()
    {
        Iterable<AnalyzerProvider> providers = AnalyzerProvider.load( AnalyzerProvider.class );
        Stream<AnalyzerProvider> stream = StreamSupport.stream( providers.spliterator(), false );
        return stream.flatMap( provider -> StreamSupport.stream( provider.getKeys().spliterator(), false ) );
    }

    @Override
    public Object getIdentityKey()
    {
        return TX_STATE_PROVIDER_KEY;
    }

    @Override
    public AuxiliaryTransactionState createNewAuxiliaryTransactionState()
    {
        return new FulltextAuxiliaryTransactionState( this, log );
    }
}
