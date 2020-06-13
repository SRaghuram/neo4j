package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.freki.Stores;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.tokenstore.GBPTreeTokenStore;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.kernel.impl.store.MetaDataStoreInterface;
import org.neo4j.kernel.impl.store.TokenStoreInterface;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.BatchingStoreInterface;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionMetaDataStore;
import org.neo4j.token.api.NamedToken;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.*;
import java.util.function.Supplier;

import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static org.neo4j.io.IOUtils.closeAll;

public class FrekiBatchStores extends BatchingStoreBase {
    Stores stores;
    PageCursorTracer cursorTracer;
    public FrekiBatchStores(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout,
                            IdGeneratorFactory idGeneratorFactory, PageCache pageCache, PageCacheTracer pageCacheTracer) {
        super(fileSystem, databaseLayout, idGeneratorFactory, pageCache);
        cursorTracer =  pageCacheTracer.createPageCursorTracer("FrekiBatch");
    }

    @Override
    public void flushAndForce( PageCursorTracer cursorTracer ) throws IOException
    {
        if (relationshipTypeTokenStore!= null)
            relationshipTypeTokenStore.flush( cursorTracer );
        if (labelTokenStore != null)
            labelTokenStore.flush( cursorTracer );
        if (propertyKeyTokenStore != null)
            propertyKeyTokenStore.flush( cursorTracer );
        stores.flushAndForce(IOLimiter.UNLIMITED, cursorTracer);
    }
    @Override
    public StoreId getStoreId() {
        return stores.metaDataStore.getStoreId();
    }

    @Override
    public List<NamedToken> getLabelTokens(PageCursorTracer cursorTracer){
        try {
            return stores.labelTokenStore.loadTokens(cursorTracer);
        } catch (IOException io)
        {
            return Collections.emptyList();
        }
    }

    @Override
    public List<NamedToken> getPropertyKeyTokens(PageCursorTracer cursorTracer){
        try {
            return stores.propertyKeyTokenStore.loadTokens(cursorTracer);
        } catch (IOException io)
        {
            return Collections.emptyList();
        }
    }

    @Override
    public List<NamedToken> getRelationshipTypeTokens(PageCursorTracer cursorTracer){
        try {
            return stores.relationshipTypeTokenStore.loadTokens(cursorTracer);
        }catch (IOException io)
        {
            return Collections.emptyList();
        }
    }

    @Override
    public List<NamedToken> getLabelTokensReadable(PageCursorTracer cursorTracer){
        try {
            return stores.labelTokenStore.loadTokens(cursorTracer);
        }catch (IOException io)
        {
            return Collections.emptyList();
        }
    }

    @Override
    public List<NamedToken> getPropertyKeyTokensReadable(PageCursorTracer cursorTracer){
        try {
            return stores.propertyKeyTokenStore.loadTokens(cursorTracer);
        }catch (IOException io)
        {
            return Collections.emptyList();
        }
    }

    @Override
    public List<NamedToken> getRelationshipTypeTokensReadable(PageCursorTracer cursorTracer) {
        try {
            return stores.relationshipTypeTokenStore.loadTokens(cursorTracer);
        }catch (IOException io)
        {
            return Collections.emptyList();
        }
    }

    @Override
    public BatchingStoreInterface initialize(Config config, IdGeneratorFactory idGeneratorFactory, LogProvider logProvider,
                                             PageCacheTracer pageCacheTracer, MemoryTracker memoryTracker, ImmutableSet<OpenOption> openOptions) throws IOException {
        this.stores = new Stores( fileSystem, databaseLayout, pageCache, idGeneratorFactory, pageCacheTracer, RecoveryCleanupWorkCollector.immediate(),
                true, null, null,  memoryTracker );
        stores.init();
        return this;
    }
    public Stores getStores()
    {
        return stores;
    }

    @Override
    public void createNew() throws IOException {

    }

    @Override
    public long getNodeRecordSize() {
        return stores.mainStore.recordDataSize();
    }

    @Override
    public long getRelationshipRecordSize() {
        return stores.mainStore.recordSizeExponential();
    }

    @Override
    public MemoryStatsVisitor.Visitable getStoreForMemoryCalculation() {
        return null;
    }

    @Override
    public long getRelationshipStoreHighId() {
        return stores.mainStore.getHighId();
    }

    @Override
    public long getNodeStoreHighId() {
        return stores.mainStore.getHighId();
    }

    @Override
    public long getTemporaryRelationshipGroupStoreHighId() {
        return 0;
    }

    @Override
    public boolean usesDoubleRelationshipRecordUnits() {
        return false;
    }

    @Override
    public Step createDeleteDuplicateNodesStep(StageControl control, Configuration config, LongIterator nodeIds, BatchingStoreBase neoStore, DataImporterMonitor storeMonitor, PageCacheTracer pageCacheTracer) {
        return new FrekiDeleteDuplicateNodeStep( control, config, nodeIds, neoStore, storeMonitor, pageCacheTracer );
    }

    @Override
    public PageCache getPageCache() {
        return this.pageCache;
    }

    @Override
    public Supplier getRelationshipSupplier() {
        return null;
    }

    @Override
    public void markHighIds() {

    }

    @Override
    public TransactionMetaDataStore getMetaDataStore() {
        return stores.metaDataStore;
    }

    private BatchingTokenStore propertyKeyTokenStore = null;
    private BatchingTokenStore labelTokenStore = null;
    private BatchingTokenStore relationshipTypeTokenStore = null;
    @Override
    public TokenStoreInterface getPropertyKeyRepository() {
        if (propertyKeyTokenStore == null)
            propertyKeyTokenStore = new BatchingTokenStore(stores.propertyKeyTokenStore, cursorTracer);
        return propertyKeyTokenStore;
    }

    @Override
    public TokenStoreInterface getLabelRepository() {
        if (labelTokenStore == null)
            labelTokenStore = new BatchingTokenStore(stores.labelTokenStore, cursorTracer);
        return labelTokenStore;
    }

    @Override
    public TokenStoreInterface getRelationshipTypeRepository() {
        if (relationshipTypeTokenStore == null)
            relationshipTypeTokenStore = new BatchingTokenStore(stores.relationshipTypeTokenStore, cursorTracer);
        return relationshipTypeTokenStore;
    }

    @Override
    public void close() throws IOException {
// Here as a safety mechanism when e.g. panicking.
        markHighIds();
        //if ( flusher != null )
        //{
        //    stopFlushingPageCache();
        //}
        getRelationshipTypeRepository().flush( cursorTracer );
        getLabelRepository().flush( cursorTracer );
        getPropertyKeyRepository().flush( cursorTracer );
        stores.flushAndForce(IOLimiter.UNLIMITED, cursorTracer);
        // Close the neo store
        //life.shutdown();
        //closeAll( neoStores, temporaryNeoStores );
        //if ( !externalPageCache )
        //{
        //    pageCache.close();
        //}

        if ( successful )
        {
            //cleanup();
        }
    }

    @Override
    public void acceptMemoryStatsVisitor(MemoryStatsVisitor visitor) {

    }

    public class BatchingTokenStore implements TokenStoreInterface
    {
        GBPTreeTokenStore treeTokenStore;
        private final Map<String,Integer> tokens = new HashMap<>();
        private int highId;
        private int highestCreatedId;
        public BatchingTokenStore(GBPTreeTokenStore treeTokenStore, PageCursorTracer cursorTracer)
        {
            this.treeTokenStore = treeTokenStore;
            try {
                List<NamedToken> tokens = treeTokenStore.loadTokens(cursorTracer);
            } catch (IOException io)
            {
                throw new UnderlyingStorageException(io.getMessage());
            }
            highId = treeTokenStore.getHighId();
            this.highestCreatedId = highId - 1;
        }

        @Override
        public int nextTokenId(PageCursorTracer cursorTracer) {
            return treeTokenStore.nextTokenId(cursorTracer);
        }

        /**
         * Returns the id for token with the specified {@code name}, potentially creating that token and
         * assigning a new id as part of this call.
         *
         * @param name token name.
         * @return the id (created or existing) for the token by this name.
         */
        public int getOrCreateId( String name )
        {
            assert name != null;
            Integer id = tokens.get( name );
            if ( id == null )
            {
                synchronized ( tokens )
                {
                    id = tokens.computeIfAbsent( name, k -> highId++ );
                }
            }
            return id;
        }

        public long[] getOrCreateIds( String[] names )
        {
            return getOrCreateIds( names, names.length );
        }

        /**
         * Returns or creates multiple tokens for given token names.
         *
         * @param names token names to lookup or create token ids for.
         * @param length length of the names array to consider, the array itself may be longer.
         * @return {@code long[]} containing the label ids.
         */
        public long[] getOrCreateIds( String[] names, int length )
        {
            long[] result = new long[length];
            int from;
            int to;
            for ( from = 0, to = 0; from < length; from++ )
            {
                int id = getOrCreateId( names[from] );
                if ( !contains( result, id, to ) )
                {
                    result[to++] = id;
                }
            }
            if ( to < from )
            {
                result = Arrays.copyOf( result, to );
            }
            Arrays.sort( result );
            return result;
        }

        @Override
        public void flush( PageCursorTracer cursorTracer ) {
            int highest = highestCreatedId;
            for ( Map.Entry<Integer,String> tokenToCreate : sortCreatedTokensById() )
            {
                if ( tokenToCreate.getKey() > highestCreatedId )
                {
                    createToken( tokenToCreate.getValue(), tokenToCreate.getKey(), cursorTracer );
                    highest = Math.max( highest, tokenToCreate.getKey() );
                }
            }
            // Store them
            int highestId = max( toIntExact( treeTokenStore.getHighId() ), highest );
            treeTokenStore.setHighId( highestId );
            highestCreatedId = highestId;
        }

        @Override
        public int getHighId() {
            return highestCreatedId;
        }

        private void createToken( String name, int tokenId, PageCursorTracer cursorTracer ) {
            NamedToken token = new NamedToken( name, tokenId);
            try
            {
                treeTokenStore.writeToken( token, cursorTracer );
            } catch (IOException io)
            {
                throw new UnderlyingStorageException(io.getMessage());
            }
        }

        private boolean contains( long[] array, long id, int arrayLength )
        {
            for ( int i = 0; i < arrayLength; i++ )
            {
                if ( array[i] == id )
                {
                    return true;
                }
            }
            return false;
        }
        private Iterable<Map.Entry<Integer,String>> sortCreatedTokensById()
        {
            Map<Integer,String> sorted = new TreeMap<>();
            for ( Map.Entry<String,Integer> entry : tokens.entrySet() )
            {
                sorted.put( entry.getValue(), entry.getKey() );
            }
            return sorted.entrySet();
        }
    }
    boolean successful;
    public void success()
    {
        successful = true;
    }

}
