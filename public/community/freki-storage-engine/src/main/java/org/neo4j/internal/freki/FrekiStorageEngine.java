/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import org.eclipse.collections.api.factory.Sets;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Collection;

import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.kernel.api.exceptions.TransactionApplyKernelException;
import org.neo4j.internal.metadatastore.GBPTreeMetaDataStore;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.internal.schemastore.GBPTreeSchemaStore;
import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.NodeLabelUpdateListener;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;

import static org.neo4j.internal.helpers.ArrayUtil.concat;
import static org.neo4j.io.IOUtils.closeAllSilently;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onInit;

public class FrekiStorageEngine implements StorageEngine
{
    private static final int MAX_TOKEN_ID = (int) ((1L << 24) - 1);

    private final FileSystemAbstraction fs;
    private final DatabaseLayout databaseLayout;
    private final Config config;
    private final PageCache pageCache;
    private final TokenHolders tokenHolders;
    private final SchemaState schemaState;
    private final ConstraintRuleAccessor constraintSemantics;
    private final IndexConfigCompleter indexConfigCompleter;
    private final LockService lockService;
    private final IdGeneratorFactory idGeneratorFactory;
    private final IdController idController;
    private final DatabaseHealth databaseHealth;
    private final LogProvider logProvider;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final boolean createStoreIfNotExists;
    private final PageCacheTracer pageCacheTracer;
    private final PageCursorTracerSupplier cursorTracerSupplier;
    private final LifeSupport life = new LifeSupport();

    private final Stores stores;
    private IndexUpdateListener indexUpdateListener;
    private NodeLabelUpdateListener nodeLabelUpdateListener;

    FrekiStorageEngine( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, LogProvider logProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean createStoreIfNotExists,
            PageCacheTracer pageCacheTracer, PageCursorTracerSupplier cursorTracerSupplier )
    {
        this.fs = fs;
        this.databaseLayout = databaseLayout;
        this.config = config;
        this.pageCache = pageCache;
        this.tokenHolders = tokenHolders;
        this.schemaState = schemaState;
        this.constraintSemantics = constraintSemantics;
        this.indexConfigCompleter = indexConfigCompleter;
        this.lockService = lockService;
        this.idGeneratorFactory = idGeneratorFactory;
        this.idController = idController;
        this.databaseHealth = databaseHealth;
        this.logProvider = logProvider;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.createStoreIfNotExists = createStoreIfNotExists;
        this.pageCacheTracer = pageCacheTracer;
        this.cursorTracerSupplier = cursorTracerSupplier;

        SimpleStore[] mainStores = new SimpleStore[4];
        BigPropertyValueStore bigPropertyValueStore = null;
        DenseStore denseStore = null;
        IdGenerator relationshipsIdGenerator = null;
        GBPTreeMetaDataStore metaDataStore = null;
        GBPTreeCountsStore countsStore = null;
        GBPTreeSchemaStore schemaStore = null;
        GBPTreeTokenStore propertyKeyTokenStore = null;
        GBPTreeTokenStore relationshipTypeTokenStore = null;
        GBPTreeTokenStore labelTokenStore = null;
        boolean success = false;
        try
        {
            if ( createStoreIfNotExists )
            {
                fs.mkdirs( databaseLayout.databaseDirectory() );
            }
            mainStores[0] = new Store( fs, databaseLayout.file( "main-store-x1" ), pageCache, idGeneratorFactory, IdType.NODE, false, createStoreIfNotExists, 0,
                    cursorTracerSupplier );
            for ( int i = 1; i < mainStores.length; i++ )
            {
                mainStores[i] = new Store( fs, databaseLayout.file( "main-store-x" + (1 << i) ), pageCache,
                        new DefaultIdGeneratorFactory( fs, recoveryCleanupWorkCollector, false ), IdType.NODE, false, createStoreIfNotExists, i,
                        cursorTracerSupplier );
            }
            bigPropertyValueStore =
                    new BigPropertyValueStore( fs, databaseLayout.file( "big-values" ), pageCache, false, createStoreIfNotExists, cursorTracerSupplier );
            denseStore = new DenseStore( pageCache, databaseLayout.file( "dense-store" ), recoveryCleanupWorkCollector, false, pageCacheTracer );
            relationshipsIdGenerator =
                    idGeneratorFactory.create( pageCache, databaseLayout.relationshipStore(), IdType.RELATIONSHIP, 0, false, Long.MAX_VALUE, false,
                            cursorTracerSupplier.get(), Sets.immutable.empty() );
            PageCursorTracer cursorTracer = cursorTracerSupplier.get();
            metaDataStore =
                    new GBPTreeMetaDataStore( pageCache, databaseLayout.file( Stores.META_DATA_STORE_FILENAME ), 123456789, false, pageCacheTracer, cursorTracer );
            countsStore = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), recoveryCleanupWorkCollector,
                    initialCountsBuilder( metaDataStore ), false, pageCacheTracer, GBPTreeCountsStore.NO_MONITOR );
            schemaStore = new GBPTreeSchemaStore( pageCache, databaseLayout.schemaStore(), recoveryCleanupWorkCollector, idGeneratorFactory, false,
                    pageCacheTracer, cursorTracer );
            propertyKeyTokenStore = new GBPTreeTokenStore( pageCache, databaseLayout.propertyKeyTokenStore(), recoveryCleanupWorkCollector,
                    idGeneratorFactory, IdType.PROPERTY_KEY_TOKEN, MAX_TOKEN_ID, false, pageCacheTracer, cursorTracer );
            relationshipTypeTokenStore = new GBPTreeTokenStore( pageCache, databaseLayout.relationshipTypeTokenStore(), recoveryCleanupWorkCollector,
                    idGeneratorFactory, IdType.RELATIONSHIP_TYPE_TOKEN, MAX_TOKEN_ID, false, pageCacheTracer, cursorTracer );
            labelTokenStore = new GBPTreeTokenStore( pageCache, databaseLayout.labelTokenStore(), recoveryCleanupWorkCollector,
                    idGeneratorFactory, IdType.LABEL_TOKEN, MAX_TOKEN_ID, false, pageCacheTracer, cursorTracer );
            SchemaCache schemaCache = new SchemaCache( constraintSemantics, indexConfigCompleter );
            this.stores = new Stores( mainStores, bigPropertyValueStore, denseStore, relationshipsIdGenerator, metaDataStore, countsStore, schemaStore,
                    schemaCache, propertyKeyTokenStore, relationshipTypeTokenStore, labelTokenStore );
            life.add( stores );
            success = true;
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            if ( !success )
            {
                closeAllSilently( concat( mainStores, bigPropertyValueStore, denseStore, relationshipsIdGenerator, metaDataStore, countsStore, schemaStore,
                        propertyKeyTokenStore, relationshipTypeTokenStore, labelTokenStore ) );
            }
        }
    }

    private CountsBuilder initialCountsBuilder( GBPTreeMetaDataStore metaDataStore )
    {
        return new CountsBuilder()
        {
            @Override
            public void initialize( CountsAccessor.Updater updater )
            {
                // TODO rebuild from store, right?
            }

            @Override
            public long lastCommittedTxId()
            {
                return metaDataStore.getLastCommittedTransactionId();
            }
        };
    }

    @Override
    public CommandCreationContext newCommandCreationContext( PageCursorTracer cursorTracer )
    {
        return new FrekiCommandCreationContext( idGeneratorFactory, cursorTracer );
    }

    @Override
    public void addIndexUpdateListener( IndexUpdateListener indexUpdateListener )
    {
        this.indexUpdateListener = indexUpdateListener;
    }

    @Override
    public void addNodeLabelUpdateListener( NodeLabelUpdateListener nodeLabelUpdateListener )
    {
        this.nodeLabelUpdateListener = nodeLabelUpdateListener;
    }

    @Override
    public void createCommands( Collection<StorageCommand> target, ReadableTransactionState state, StorageReader storageReader,
            CommandCreationContext creationContext, ResourceLocker locks, long lastTransactionIdWhenStarted, TxStateVisitor.Decorator additionalTxStateVisitor,
            PageCursorTracer cursorTracer )
            throws KernelException
    {
        try ( TxStateVisitor visitor = additionalTxStateVisitor.apply( new CommandCreator( target, stores ) ) )
        {
            state.accept( visitor );
        }
    }

    @Override
    public void apply( CommandsToApply batch, TransactionApplicationMode mode ) throws Exception
    {
        // Have these command appliers as separate try-with-resource to have better control over
        // point between closing this and the locks above
        CommandsToApply initialBatch = batch;
        try ( LockGroup locks = new LockGroup();
              FrekiTransactionApplier txApplier = new FrekiTransactionApplier( stores, indexUpdateListener ) )
        {
            while ( batch != null )
            {
                batch.accept( txApplier );
                batch = batch.next();
            }
        }
        catch ( Throwable cause )
        {
            TransactionApplyKernelException kernelException = new TransactionApplyKernelException(
                    cause, "Failed to apply transaction: %s", batch == null ? initialBatch : batch );
            databaseHealth.panic( kernelException );
            throw kernelException;
        }
    }

    @Override
    public void flushAndForce( IOLimiter limiter, PageCursorTracer cursorTracer ) throws IOException
    {
        stores.flushAndForce( limiter, cursorTracer );
        pageCache.flushAndForce( limiter );
    }

    @Override
    public void dumpDiagnostics( DiagnosticsManager diagnosticsManager, Log log )
    {
    }

    @Override
    public void forceClose()
    {
        shutdown();
    }

    @Override
    public Collection<StoreFileMetadata> listStorageFiles()
    {
        return null;
    }

    @Override
    public StoreId getStoreId()
    {
        return stores.metaDataStore.getStoreId();
    }

    @Override
    public Lifecycle schemaAndTokensLifecycle()
    {
        return onInit( () -> loadTokensAndSchema( cursorTracerSupplier.get() ) );
    }

    private void loadTokensAndSchema( PageCursorTracer cursorTracer ) throws Exception
    {
        tokenHolders.labelTokens().setInitialTokens( stores.labelTokenStore.loadTokens( cursorTracer ) );
        tokenHolders.relationshipTypeTokens().setInitialTokens( stores.relationshipTypeTokenStore.loadTokens( cursorTracer ) );
        tokenHolders.propertyKeyTokens().setInitialTokens( stores.propertyKeyTokenStore.loadTokens( cursorTracer ) );
        stores.schemaCache.load( stores.schemaStore.loadRules( cursorTracer ) );
    }

    @Override
    public TransactionIdStore transactionIdStore()
    {
        return stores.metaDataStore;
    }

    @Override
    public LogVersionRepository logVersionRepository()
    {
        return stores.metaDataStore;
    }

    @Override
    public CountsAccessor countsAccessor()
    {
        return stores.countsStore;
    }

    @Override
    public StorageReader newReader()
    {
        return new FrekiStorageReader( stores, tokenHolders );
    }

    @Override
    public void init()
    {
        life.init();
    }

    @Override
    public void start()
    {
        life.start();
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    @Override
    public void shutdown()
    {
        life.shutdown();
    }

    public void printAverageFactorFilledRecords() throws IOException
    {
        for ( int i = 0; i < 4; i++ )
        {
            SimpleStore store = stores.mainStore( i );
            if ( store != null )
            {
                System.out.println( averageFactorFilledRecords( store ) );
            }
        }
    }

    private double averageFactorFilledRecords( SimpleStore store )
    {
        long bytesUsed = 0;
        long bytesMax = 9;
        Record record = store.newRecord();
        try ( PageCursor cursor = store.openReadCursor() )
        {
            long highId = store.getHighId();
            for ( long id = 0; id < highId; id++ )
            {
                if ( store.read( cursor, record, id ) )
                {
                    MutableNodeRecordData data = new MutableNodeRecordData( id );
                    ByteBuffer buffer = record.dataForReading();
                    data.deserialize( buffer );
                    bytesUsed += buffer.position();
                    bytesMax += buffer.capacity();
                }
            }
        }
        return ((double) bytesUsed) / bytesMax;
    }
}
