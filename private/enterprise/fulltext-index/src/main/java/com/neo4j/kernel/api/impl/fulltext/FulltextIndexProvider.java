/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import com.neo4j.kernel.api.impl.fulltext.lucene.DatabaseFulltextIndex;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexAccessor;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexBuilder;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexPopulator;
import com.neo4j.kernel.api.impl.fulltext.lucene.FulltextIndexReader;
import com.neo4j.kernel.api.impl.fulltext.lucene.LuceneFulltextDocumentStructure;
import com.neo4j.kernel.api.impl.fulltext.lucene.ScoreEntityIterator;
import org.apache.lucene.queryparser.classic.ParseException;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.graphdb.index.fulltext.AnalyzerProvider;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.io.IOUtils;
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
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionState;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.api.txstate.aux.AuxiliaryTransactionStateProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.newapi.AllStoreHolder;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.values.storable.Value;

class FulltextIndexProvider extends AbstractLuceneIndexProvider implements FulltextAdapter, AuxiliaryTransactionStateProvider
{
    private static final String TX_PROVIDER_KEY = "FULLTEXT SCHEMA INDEX TRANSACTION STATE";

    private final FileSystemAbstraction fileSystem;
    private final Config config;
    private final TokenHolders tokenHolders;
    private final OperationalMode operationalMode;
    private final String defaultAnalyzerName;
    private final String defaultEventuallyConsistentSetting;
    private final AuxiliaryTransactionStateManager auxiliaryTransactionStateManager;
    private final IndexUpdateSink indexUpdateSink;
    private final ConcurrentMap<StoreIndexDescriptor,FulltextIndexAccessor> openOnlineAccessors;

    FulltextIndexProvider( IndexProviderDescriptor descriptor, IndexDirectoryStructure.Factory directoryStructureFactory,
            FileSystemAbstraction fileSystem, Config config, TokenHolders tokenHolders, DirectoryFactory directoryFactory, OperationalMode operationalMode,
            JobScheduler scheduler, AuxiliaryTransactionStateManager auxiliaryTransactionStateManager )
    {
        super( descriptor, directoryStructureFactory, config, operationalMode, fileSystem, directoryFactory );
        this.fileSystem = fileSystem;
        this.config = config;
        this.tokenHolders = tokenHolders;
        this.operationalMode = operationalMode;

        defaultAnalyzerName = config.get( FulltextConfig.fulltext_default_analyzer );
        defaultEventuallyConsistentSetting = Boolean.toString( config.get( FulltextConfig.eventually_consistent ) );
        this.auxiliaryTransactionStateManager = auxiliaryTransactionStateManager;
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
        return accessor;
    }

    private FulltextIndexAccessor getOpenOnlineAccessor( StoreIndexDescriptor descriptor )
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
        if ( kti.hasTxStateWithChanges() )
        {
            FulltextTransactionState auxiliaryTxState = (FulltextTransactionState) allStoreHolder.auxiliaryTxState( TX_PROVIDER_KEY );
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
        return TX_PROVIDER_KEY;
    }

    @Override
    public AuxiliaryTransactionState createNewAuxiliaryTransactionState()
    {
        return new FulltextTransactionState( this );
    }

    private static class FulltextTransactionState implements AuxiliaryTransactionState, Function<IndexReference,IndexTxState>
    {
        private final FulltextIndexProvider fulltextIndexProvider;
        private final Map<IndexReference,IndexTxState> indexStates;

        private FulltextTransactionState( FulltextIndexProvider fulltextIndexProvider )
        {
            this.fulltextIndexProvider = fulltextIndexProvider;
            indexStates = new HashMap<>();
        }

        @Override
        public void close() throws Exception
        {
            IOUtils.closeAll( indexStates.values() );
        }

        @Override
        public boolean hasChanges()
        {
            // We always return 'false' here, because we only use this transaction state for reading.
            //Our index changes are already derived from the store commands, so we never have any commands of our own to extract.
            return false;
        }

        @Override
        public void extractCommands( Collection<StorageCommand> target )
        {
            // We never have any commands to extract, because this transaction state is only used for reading.
        }

        FulltextIndexReader indexReader( IndexReference indexReference, KernelTransactionImplementation kti )
        {
            IndexTxState state = indexStates.computeIfAbsent( indexReference, this );
            return state.getIndexReader( kti );
        }

        @Override
        public IndexTxState apply( IndexReference indexReference )
        {
            return new IndexTxState( fulltextIndexProvider, indexReference );
        }
    }

    private static class IndexTxState implements Closeable
    {
        private final FulltextIndexProvider provider;
        private final StoreIndexDescriptor descriptor;
        private final FulltextIndexAccessor accessor;
        private final List<AutoCloseable> toCloseLater;
        private final FulltextIndexAccessor.FulltextIndexTransactionStateUpdater updater;
        private FulltextIndexReader currentReader;
        private long lastUpdateRevision;
        private final SchemaDescriptor schema;
        private final boolean visitingNodes;
        private final int[] propertyIds;
        private final Value[] propertyValues;
        private final IntIntHashMap propKeyToIndex;

        private IndexTxState( FulltextIndexProvider fulltextIndexProvider, IndexReference indexReference )
        {
            provider = fulltextIndexProvider;
            descriptor = (StoreIndexDescriptor) indexReference;
            accessor = provider.getOpenOnlineAccessor( descriptor );
            toCloseLater = new ArrayList<>();
            updater = accessor.getTransactionStateIndexUpdater();
            schema = descriptor.schema();
            visitingNodes = schema.entityType() == EntityType.NODE;
            propertyIds = schema.getPropertyIds();
            propertyValues = new Value[propertyIds.length];
            propKeyToIndex = new IntIntHashMap();
            for ( int i = 0; i < propertyIds.length; i++ )
            {
                propKeyToIndex.put( propertyIds[i], i );
            }
        }

        FulltextIndexReader getIndexReader( KernelTransactionImplementation kti )
        {
            if ( currentReader == null || lastUpdateRevision != kti.getTransactionDataRevision() )
            {
                if ( currentReader != null )
                {
                    toCloseLater.add( currentReader );
                }
                try
                {
                    updateReader( kti );
                }
                catch ( Exception e )
                {
                    currentReader = null;
                    throw new RuntimeException( "Failed to update the fulltext schema index transaction state.", e );
                }
            }
            return currentReader;
        }

        private void updateReader( KernelTransactionImplementation kti ) throws Exception
        {
            updater.resetUpdaterState();
            AllStoreHolder read = (AllStoreHolder) kti.dataRead();
            TransactionState transactionState = kti.txState();

            try ( NodeCursor nodeCursor = visitingNodes ? kti.cursors().allocateNodeCursor() : null;
                  RelationshipScanCursor relationshipCursor = visitingNodes ? null : kti.cursors().allocateRelationshipScanCursor();
                  PropertyCursor propertyCursor = kti.cursors().allocatePropertyCursor() )
            {
                transactionState.accept( new TxStateVisitor.Adapter()
                {
                    @Override
                    public void visitCreatedNode( long id )
                    {
                        indexNode( id );
                    }

                    @Override
                    public void visitCreatedRelationship( long id, int type, long startNode, long endNode )
                    {
                        indexRelationship( id );
                    }

                    @Override
                    public void visitNodePropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
                    {
                        indexNode( id );
                    }

                    @Override
                    public void visitRelPropertyChanges( long id, Iterator<StorageProperty> added, Iterator<StorageProperty> changed, IntIterable removed )
                    {
                        indexRelationship( id );
                    }

                    @Override
                    public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
                    {
                        indexNode( id );
                    }

                    private void indexNode( long id )
                    {
                        if ( visitingNodes )
                        {
                            read.singleNode( id, nodeCursor );
                            if ( nodeCursor.next() )
                            {
                                LabelSet labels = nodeCursor.labels();
                                if ( schema.isAffected( labels.all() ) )
                                {
                                    nodeCursor.properties( propertyCursor );
                                    indexProperties( id );
                                }
                            }
                        }
                    }

                    private void indexRelationship( long id )
                    {
                        if ( !visitingNodes )
                        {
                            read.singleRelationship( id, relationshipCursor );
                            if ( relationshipCursor.next() && schema.isAffected( new long[] {relationshipCursor.type()} ) )
                            {
                                relationshipCursor.properties( propertyCursor );
                                indexProperties( id );
                            }
                        }
                    }

                    private void indexProperties( long id )
                    {
                        while ( propertyCursor.next() )
                        {
                            int propertyKey = propertyCursor.propertyKey();
                            int index = propKeyToIndex.getIfAbsent( propertyKey, -1 );
                            if ( index != -1 )
                            {
                                propertyValues[index] = propertyCursor.propertyValue();
                            }
                        }
                        updater.add( id, propertyValues );
                        Arrays.fill( propertyValues, null );
                    }
                });
            }
            IndexReader baseReader = read.indexReader( descriptor, false );
            currentReader = updater.getTransactionStateIndexReader( (FulltextIndexReader) baseReader );
            lastUpdateRevision = kti.getTransactionDataRevision();
        }

        @Override
        public void close() throws IOException
        {
            toCloseLater.add( currentReader );
            toCloseLater.add( updater );
            IOUtils.closeAll( toCloseLater );
        }
    }
}
