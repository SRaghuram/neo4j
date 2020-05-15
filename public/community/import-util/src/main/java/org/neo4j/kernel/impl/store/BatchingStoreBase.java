package org.neo4j.kernel.impl.store;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.counts.CountsBuilder;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.BatchingStoreInterface;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.token.api.NamedToken;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.neo4j.io.pagecache.IOLimiter.UNLIMITED;

public abstract class BatchingStoreBase implements AutoCloseable, BatchingStoreInterface {

    protected DatabaseLayout databaseLayout;
    protected PageCache pageCache;
    protected FileSystemAbstraction fileSystem;
    protected IdGeneratorFactory idGeneratorFactory;
    public BatchingStoreBase(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, IdGeneratorFactory idGeneratorFactory, PageCache pageCache)
    {
        this.databaseLayout = databaseLayout;
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
        this.idGeneratorFactory = idGeneratorFactory;
    }

    public void buildCountsStore(CountsBuilder builder, PageCacheTracer cacheTracer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker)
    {
        try ( GBPTreeCountsStore countsStore = new GBPTreeCountsStore( pageCache, databaseLayout.countStore(), fileSystem,
                RecoveryCleanupWorkCollector.immediate(), builder, false, cacheTracer, GBPTreeCountsStore.NO_MONITOR ) )
        {
            countsStore.start( cursorTracer, memoryTracker );
            countsStore.checkpoint( UNLIMITED, cursorTracer );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public IdGeneratorFactory getIdGeneratorFactory() {
        return idGeneratorFactory;
    }
    public abstract StoreId getStoreId();
    public abstract List<NamedToken> getLabelTokens(PageCursorTracer cursorTracer);
    public abstract List<NamedToken> getPropertyKeyTokens(PageCursorTracer cursorTracer);
    public abstract List<NamedToken> getRelationshipTypeTokens(PageCursorTracer cursorTracer);
    public abstract List<NamedToken> getLabelTokensReadable(PageCursorTracer cursorTracer);
    public abstract List<NamedToken> getPropertyKeyTokensReadable(PageCursorTracer cursorTracer);
    public abstract List<NamedToken> getRelationshipTypeTokensReadable(PageCursorTracer cursorTracer);
    public abstract void flushAndForce( PageCursorTracer cursorTracer ) throws IOException;
    public abstract void success();
}
