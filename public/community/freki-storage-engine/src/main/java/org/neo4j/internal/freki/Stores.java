package org.neo4j.internal.freki;

import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.freki.store.Store;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schemastore.GBPTreeSchemaStore;
import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.TransactionMetaDataStore;

class Stores extends LifecycleAdapter
{
    private final LifeSupport life = new LifeSupport();
    final Store mainStore;
    final TransactionMetaDataStore metaDataStore;
    final GBPTreeCountsStore countsStore;
    final GBPTreeSchemaStore schemaStore;
    final SchemaCache schemaCache;
    final GBPTreeTokenStore propertyKeyTokenStore;
    final GBPTreeTokenStore relationshipTypeTokenStore;
    final GBPTreeTokenStore labelTokenStore;

    Stores( Store mainStore, TransactionMetaDataStore metaDataStore, GBPTreeCountsStore countsStore, GBPTreeSchemaStore schemaStore, SchemaCache schemaCache,
            GBPTreeTokenStore propertyKeyTokenStore, GBPTreeTokenStore relationshipTypeTokenStore, GBPTreeTokenStore labelTokenStore )
    {
        this.mainStore = mainStore;
        this.metaDataStore = metaDataStore;
        this.countsStore = countsStore;
        this.schemaStore = schemaStore;
        this.schemaCache = schemaCache;
        this.propertyKeyTokenStore = propertyKeyTokenStore;
        this.relationshipTypeTokenStore = relationshipTypeTokenStore;
        this.labelTokenStore = labelTokenStore;
        life.add( mainStore );
        life.add( onShutdown( metaDataStore::close ) );
        life.add( onShutdown( countsStore::close ) );
        life.add( onShutdown( schemaStore::close ) );
        life.add( onShutdown( propertyKeyTokenStore::close ) );
        life.add( onShutdown( relationshipTypeTokenStore::close ) );
        life.add( onShutdown( labelTokenStore::close ) );
    }

    void flushAndForce( IOLimiter limiter, PageCursorTracer cursorTracer )
    {
        mainStore.flush( cursorTracer );
        metaDataStore.flush( cursorTracer );
        schemaStore.checkpoint( limiter, cursorTracer );
        propertyKeyTokenStore.checkpoint( limiter, cursorTracer );
        relationshipTypeTokenStore.checkpoint( limiter, cursorTracer );
        labelTokenStore.checkpoint( limiter, cursorTracer );
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
}
