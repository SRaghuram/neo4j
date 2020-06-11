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

import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.metadatastore.GBPTreeMetaDataStore;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schemastore.GBPTreeSchemaStore;
import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.token.api.NamedToken;

import static org.neo4j.internal.helpers.ArrayUtil.concat;
import static org.neo4j.io.IOUtils.closeAllSilently;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onShutdown;

public class Stores extends MainStores
{
    private static final int MAX_TOKEN_ID = (int) ((1L << 24) - 1);
    static final String META_DATA_STORE_FILENAME = "meta-data-store";

    public final TransactionMetaDataStore metaDataStore;
    public final GBPTreeCountsStore countsStore;
    public final GBPTreeSchemaStore schemaStore;
    public final SchemaCache schemaCache;
    public final GBPTreeTokenStore propertyKeyTokenStore;
    public final GBPTreeTokenStore relationshipTypeTokenStore;
    public final GBPTreeTokenStore labelTokenStore;
    private final MemoryTracker memoryTracker;

    public Stores( FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, IdGeneratorFactory idGeneratorFactory,
            PageCacheTracer pageCacheTracer, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean createStoreIfNotExists, ConstraintRuleAccessor constraintSemantics, IndexConfigCompleter indexConfigCompleter, MemoryTracker memoryTracker )
            throws IOException
    {
        super( fs, databaseLayout, pageCache, idGeneratorFactory, pageCacheTracer, recoveryCleanupWorkCollector, createStoreIfNotExists );
        this.memoryTracker = memoryTracker;
        GBPTreeMetaDataStore metaDataStore = null;
        GBPTreeCountsStore countsStore = null;
        GBPTreeSchemaStore schemaStore = null;
        GBPTreeTokenStore propertyKeyTokenStore = null;
        GBPTreeTokenStore relationshipTypeTokenStore = null;
        GBPTreeTokenStore labelTokenStore = null;
        boolean success = false;
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "Open stores" ) )
        {
            metaDataStore = FrekiStorageEngineFactory.openMetaDataStore( databaseLayout, pageCache, pageCacheTracer );
            countsStore = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), fs, recoveryCleanupWorkCollector,
                    initialCountsBuilder( metaDataStore ), false, pageCacheTracer, GBPTreeCountsStore.NO_MONITOR );
            propertyKeyTokenStore = new GBPTreeTokenStore( pageCache, databaseLayout.propertyKeyTokenStore(), recoveryCleanupWorkCollector,
                    idGeneratorFactory, IdType.PROPERTY_KEY_TOKEN, MAX_TOKEN_ID, false, pageCacheTracer, cursorTracer );
            idGeneratorsToRegisterOnTheWorkSync.add( Pair.of( idGeneratorFactory, IdType.PROPERTY_KEY_TOKEN ) );
            relationshipTypeTokenStore = new GBPTreeTokenStore( pageCache, databaseLayout.relationshipTypeTokenStore(), recoveryCleanupWorkCollector,
                    idGeneratorFactory, IdType.RELATIONSHIP_TYPE_TOKEN, MAX_TOKEN_ID, false, pageCacheTracer, cursorTracer );
            idGeneratorsToRegisterOnTheWorkSync.add( Pair.of( idGeneratorFactory, IdType.RELATIONSHIP_TYPE_TOKEN ) );
            labelTokenStore = new GBPTreeTokenStore( pageCache, databaseLayout.labelTokenStore(), recoveryCleanupWorkCollector,
                    idGeneratorFactory, IdType.LABEL_TOKEN, MAX_TOKEN_ID, false, pageCacheTracer, cursorTracer );
            idGeneratorsToRegisterOnTheWorkSync.add( Pair.of( idGeneratorFactory, IdType.LABEL_TOKEN ) );
            schemaStore = new GBPTreeSchemaStore( pageCache, databaseLayout.schemaStore(), recoveryCleanupWorkCollector, idGeneratorFactory,
                    tokenNameLookup( propertyKeyTokenStore, relationshipTypeTokenStore, labelTokenStore, cursorTracer ), false, pageCacheTracer, cursorTracer );
            idGeneratorsToRegisterOnTheWorkSync.add( Pair.of( idGeneratorFactory, IdType.SCHEMA ) );
            SchemaCache schemaCache = new SchemaCache( constraintSemantics, indexConfigCompleter );
            success = true;

            this.metaDataStore = metaDataStore;
            this.countsStore = countsStore;
            this.schemaStore = schemaStore;
            this.schemaCache = schemaCache;
            this.propertyKeyTokenStore = propertyKeyTokenStore;
            this.relationshipTypeTokenStore = relationshipTypeTokenStore;
            this.labelTokenStore = labelTokenStore;
            addStoresToLife();
        }
        finally
        {
            if ( !success )
            {
                closeAllSilently( concat( metaDataStore, countsStore, schemaStore, propertyKeyTokenStore, relationshipTypeTokenStore,
                        labelTokenStore ) );
            }
        }
    }

    Stores( SimpleStore[] mainStores, BigPropertyValueStore bigPropertyValueStore, SimpleDenseRelationshipStore denseStore,
            TransactionMetaDataStore metaDataStore, GBPTreeCountsStore countsStore, GBPTreeSchemaStore schemaStore, SchemaCache schemaCache,
            GBPTreeTokenStore propertyKeyTokenStore, GBPTreeTokenStore relationshipTypeTokenStore, GBPTreeTokenStore labelTokenStore,
            MemoryTracker memoryTracker )
    {
        super( mainStores, bigPropertyValueStore, denseStore );
        this.metaDataStore = metaDataStore;
        this.countsStore = countsStore;
        this.schemaStore = schemaStore;
        this.schemaCache = schemaCache;
        this.propertyKeyTokenStore = propertyKeyTokenStore;
        this.relationshipTypeTokenStore = relationshipTypeTokenStore;
        this.labelTokenStore = labelTokenStore;
        this.memoryTracker = memoryTracker;
        addStoresToLife();
    }

    private void addStoresToLife()
    {
        life.add( onShutdown( metaDataStore::close ) );
        life.add( new LifecycleAdapter()
        {
            @Override
            public void start() throws Exception
            {
                countsStore.start( PageCursorTracer.NULL, memoryTracker );
            }

            @Override
            public void shutdown()
            {
                countsStore.close();
            }
        } );
        life.add( onShutdown( schemaStore::close ) );
        life.add( onShutdown( propertyKeyTokenStore::close ) );
        life.add( onShutdown( relationshipTypeTokenStore::close ) );
        life.add( onShutdown( labelTokenStore::close ) );
    }

    @Override
    public void flushAndForce(IOLimiter limiter, PageCursorTracer cursorTracer) throws IOException
    {
        super.flushAndForce( limiter, cursorTracer );
        metaDataStore.flush( cursorTracer );
        schemaStore.checkpoint( limiter, cursorTracer );
        propertyKeyTokenStore.checkpoint( limiter, cursorTracer );
        relationshipTypeTokenStore.checkpoint( limiter, cursorTracer );
        labelTokenStore.checkpoint( limiter, cursorTracer );
        countsStore.checkpoint( limiter, cursorTracer );
    }

    private CountsBuilder initialCountsBuilder( GBPTreeMetaDataStore metaDataStore )
    {
        return new CountsBuilder()
        {
            @Override
            public void initialize( CountsAccessor.Updater updater, PageCursorTracer tracer, MemoryTracker memoryTracker )
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

    private static TokenNameLookup tokenNameLookup( GBPTreeTokenStore propertyKeyTokens, GBPTreeTokenStore relationshipTypeTokens,
            GBPTreeTokenStore labelTokens, PageCursorTracer cursorTracer )
    {
        return new TokenNameLookup()
        {
            @Override
            public String labelGetName( int labelId )
            {
                return tokenName( labelId, id -> labelTokens.loadToken( id, cursorTracer ) );
            }

            @Override
            public String relationshipTypeGetName( int relationshipTypeId )
            {
                return tokenName( relationshipTypeId, id -> relationshipTypeTokens.loadToken( id, cursorTracer ) );
            }

            @Override
            public String propertyKeyGetName( int propertyKeyId )
            {
                return tokenName( propertyKeyId, id -> propertyKeyTokens.loadToken( id, cursorTracer ) );
            }

            private String tokenName( int id, ThrowingFunction<Integer,NamedToken,IOException> lookup )
            {
                try
                {
                    NamedToken token = lookup.apply( id );
                    return token != null ? token.name() : "<unknown token with id:" + id + ">";
                }
                catch ( IOException e )
                {
                    throw new UncheckedIOException( e );
                }
            }
        };
    }
}
