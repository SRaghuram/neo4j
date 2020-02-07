package org.neo4j.internal.batchimport;

public class ImportLogicMonitor {
    public interface Monitor {

    void doubleRelationshipRecordUnitsEnabled();

    void mayExceedNodeIdCapacity(long capacity, long estimatedCount);

    void mayExceedRelationshipIdCapacity(long capacity, long estimatedCount);

    void insufficientHeapSize(long optimalMinimalHeapSize, long heapSize);

    void abundantHeapSize(long optimalMinimalHeapSize, long heapSize);

    void insufficientAvailableMemory(long estimatedCacheSize, long optimalMinimalHeapSize, long availableMemory);
    }
}
