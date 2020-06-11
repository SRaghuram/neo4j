package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;
import org.neo4j.internal.batchimport.staging.LonelyProcessingStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.BatchingStoreBase;

public class FrekiDeleteDuplicateNodeStep extends LonelyProcessingStep
{
    public FrekiDeleteDuplicateNodeStep(StageControl control, Configuration config, LongIterator nodeIds, BatchingStoreBase store,
                                    DataImporterMonitor storeMonitor, PageCacheTracer pageCacheTracer )
    {
        super(control, "DEDUP", config);
    }

    @Override
    protected void process() {

    }
}
