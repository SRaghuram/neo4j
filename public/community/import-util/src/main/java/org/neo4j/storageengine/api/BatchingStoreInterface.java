package org.neo4j.storageengine.api;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.Configuration;
//import org.neo4j.internal.batchimport.DataImporter;
import org.neo4j.internal.batchimport.DataImporterMonitor;
import org.neo4j.internal.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.staging.Step;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
//import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.kernel.impl.store.BatchingStoreBase;
import org.neo4j.kernel.impl.store.MetaDataStoreInterface;
import org.neo4j.kernel.impl.store.TokenStoreInterface;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.util.function.Supplier;

public interface BatchingStoreInterface extends Closeable, MemoryStatsVisitor.Visitable{

    public BatchingStoreInterface initialize(Config config, IdGeneratorFactory idGeneratorFactory,
                                             LogProvider logProvider, PageCacheTracer cacheTracer, MemoryTracker memoryTracker,
                                             ImmutableSet<OpenOption> openOptions ) throws IOException ;
    public void createNew() throws IOException;
    public long getNodeRecordSize();
    public long getRelationshipRecordSize();
    public MemoryStatsVisitor.Visitable getStoreForMemoryCalculation();
    public long getRelationshipStoreHighId();
    public long getNodeStoreHighId();
    public long getTemporaryRelationshipGroupStoreHighId();
    public boolean usesDoubleRelationshipRecordUnits();
    public Step createDeleteDuplicateNodesStep(StageControl control, Configuration config, LongIterator nodeIds,
                                               BatchingStoreBase neoStore, DataImporterMonitor storeMonitor, PageCacheTracer pageCacheTracer);
    public PageCache getPageCache();
    public Supplier getRelationshipSupplier();
    public void markHighIds();
    public MetaDataStoreInterface getMetaDataStore();
    public IdGeneratorFactory getIdGeneratorFactory();

    public TokenStoreInterface getPropertyKeyRepository();
    public TokenStoreInterface getLabelRepository();
    public TokenStoreInterface getRelationshipTypeRepository();
}

