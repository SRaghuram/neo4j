/*
<<<<<<< HEAD
 * Copyright (c) 2002-2020 "Neo4j,"
=======
 * Copyright (c) 2002-2019 "Neo4j,"
>>>>>>> The skeleton of Freki storage engine
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

import java.io.IOException;
import java.util.Collection;

import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsVisitor;
import org.neo4j.exceptions.KernelException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.diagnostics.DiagnosticsManager;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.kernel.api.exceptions.TransactionApplyKernelException;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.Life;
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
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.EntityTokenUpdateListener;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TransactionCountingStateVisitor;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.storageengine.util.IdGeneratorUpdatesWorkSync;
import org.neo4j.storageengine.util.IndexUpdatesWorkSync;
import org.neo4j.storageengine.util.LabelIndexUpdatesWorkSync;
import org.neo4j.token.TokenHolders;

import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onInit;

public class FrekiStorageEngine extends Life implements StorageEngine
{
    private final PageCache pageCache;
    private final TokenHolders tokenHolders;
    private final SchemaState schemaState;
    private final ConstraintRuleAccessor constraintSemantics;
    private final IdGeneratorFactory idGeneratorFactory;
    private final DatabaseHealth databaseHealth;
    private final PageCacheTracer pageCacheTracer;
    private final CursorAccessPatternTracer cursorAccessPatternTracer;

    private final Stores stores;
    private final IdGeneratorUpdatesWorkSync idGeneratorUpdatesWorkSync;
    private final FrekiStorageReader singleReader;
    private final DenseRelationshipsWorkSync denseRelationshipsWorkSync;
    private LabelIndexUpdatesWorkSync labelIndexUpdatesWorkSync;
    private IndexUpdatesWorkSync indexUpdatesWorkSync;
    private IndexUpdateListener indexUpdateListener;

    FrekiStorageEngine( FileSystemAbstraction fs, DatabaseLayout databaseLayout, Config config, PageCache pageCache, TokenHolders tokenHolders,
            SchemaState schemaState, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, LockService lockService,
            IdGeneratorFactory idGeneratorFactory, IdController idController, DatabaseHealth databaseHealth, LogProvider logProvider,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean createStoreIfNotExists,
            PageCacheTracer pageCacheTracer, CursorAccessPatternTracer cursorAccessPatternTracer )
            throws IOException
    {
        this.pageCache = pageCache;
        this.tokenHolders = tokenHolders;
        this.schemaState = schemaState;
        this.constraintSemantics = constraintSemantics;
        this.idGeneratorFactory = idGeneratorFactory;
        this.databaseHealth = databaseHealth;
        this.pageCacheTracer = pageCacheTracer;
        this.cursorAccessPatternTracer = cursorAccessPatternTracer;
        this.idGeneratorUpdatesWorkSync = new IdGeneratorUpdatesWorkSync();
        this.stores = new Stores( fs, databaseLayout, pageCache, idGeneratorFactory, pageCacheTracer, recoveryCleanupWorkCollector,
                createStoreIfNotExists, constraintSemantics, indexConfigCompleter );
        this.singleReader = new FrekiStorageReader( stores, cursorAccessPatternTracer, tokenHolders );
        this.denseRelationshipsWorkSync = new DenseRelationshipsWorkSync( stores.denseStore );
        life.add( stores );
    }

    @Override
    public CommandCreationContext newCommandCreationContext( PageCursorTracer cursorTracer )
    {
        return new FrekiCommandCreationContext( stores, idGeneratorFactory, cursorTracer );
    }

    @Override
    public void addIndexUpdateListener( IndexUpdateListener indexUpdateListener )
    {
        this.indexUpdateListener = indexUpdateListener;
        this.indexUpdatesWorkSync = new IndexUpdatesWorkSync( indexUpdateListener );
    }

    @Override
    public void addNodeLabelUpdateListener( EntityTokenUpdateListener nodeLabelUpdateListener )
    {
        this.labelIndexUpdatesWorkSync = new LabelIndexUpdatesWorkSync( nodeLabelUpdateListener );
    }

    @Override
    public void addRelationshipTypeUpdateListener( EntityTokenUpdateListener entityTokenUpdateListener )
    {
        // TODO implement
    }

    @Override
    public void createCommands( Collection<StorageCommand> target, ReadableTransactionState state, StorageReader storageReader,
            CommandCreationContext creationContext, ResourceLocker locks, long lastTransactionIdWhenStarted, TxStateVisitor.Decorator additionalTxStateVisitor,
            PageCursorTracer cursorTracer )
            throws KernelException
    {
        TxStateVisitor main = new CommandCreator( target, stores, constraintSemantics, cursorTracer );
        TxStateVisitor withCounts = trackCounts( target, main, state, storageReader, cursorTracer );
        try ( TxStateVisitor visitor = additionalTxStateVisitor.apply( withCounts ) )
        {
            state.accept( visitor );
        }
    }

    private TxStateVisitor trackCounts( Collection<StorageCommand> target, TxStateVisitor main, ReadableTransactionState state, StorageReader storageReader,
            PageCursorTracer cursorTracer )
    {
        CountsVisitor countsVisitor = new CountsVisitor()
        {
            @Override
            public void visitNodeCount( int labelId, long count )
            {
                target.add( new FrekiCommand.NodeCount( labelId, count ) );
            }

            @Override
            public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
            {
                target.add( new FrekiCommand.RelationshipCount( startLabelId, typeId, endLabelId, count ) );
            }
        };
        return new TransactionCountingStateVisitor( main, storageReader, state, new CountsDelta(), countsVisitor, cursorTracer );
    }

    @Override
    public void apply( CommandsToApply batch, TransactionApplicationMode mode ) throws Exception
    {
        // Have these command appliers as separate try-with-resource to have better control over
        // point between closing this and the locks above
        CommandsToApply initialBatch = batch;
        try ( LockGroup locks = new LockGroup();
                FrekiTransactionApplier txApplier = new FrekiTransactionApplier( stores, singleReader, schemaState, indexUpdateListener, mode,
                        idGeneratorUpdatesWorkSync, labelIndexUpdatesWorkSync, indexUpdatesWorkSync, denseRelationshipsWorkSync,
                        pageCacheTracer, batch.cursorTracer() ) )
        {
            while ( batch != null )
            {
                txApplier.beginTx( batch.transactionId() );
                try
                {
                    batch.accept( txApplier );
                }
                finally
                {
                    txApplier.endTx();
                }
                batch = batch.next();
            }
        }
        catch ( Throwable cause )
        {
            TransactionApplyKernelException kernelException =
                    new TransactionApplyKernelException( cause, "Failed to apply transaction: %s", batch == null ? initialBatch : batch );
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
        return onInit( () ->
        {
            try ( PageCursorTracer cursorTracer = pageCacheTracer.createPageCursorTracer( "Initi tokens" ) )
            {
                loadTokensAndSchema( cursorTracer );
            }
        } );
    }

    private void loadTokensAndSchema( PageCursorTracer cursorTracer ) throws Exception
    {
        tokenHolders.labelTokens().setInitialTokens( stores.labelTokenStore.loadTokens( cursorTracer ) );
        tokenHolders.relationshipTypeTokens().setInitialTokens( stores.relationshipTypeTokenStore.loadTokens( cursorTracer ) );
        tokenHolders.propertyKeyTokens().setInitialTokens( stores.propertyKeyTokenStore.loadTokens( cursorTracer ) );
        stores.schemaCache.load( stores.schemaStore.loadRules( cursorTracer ) );
    }

    public FrekiAnalysis analysis()
    {
        return new FrekiAnalysis( stores );
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
    public FrekiStorageReader newReader()
    {
        return singleReader;
    }

    Stores stores()
    {
        return stores;
    }

    @Override
    public void init()
    {
        super.init();
        // Now that all stores have been initialized and all id generators are opened then register them at the work-sync
        stores.idGenerators( idGeneratorUpdatesWorkSync::add );
    }

    @Override
    public void shutdown()
    {
        super.shutdown();
        cursorAccessPatternTracer.printSummary();
    }
}
