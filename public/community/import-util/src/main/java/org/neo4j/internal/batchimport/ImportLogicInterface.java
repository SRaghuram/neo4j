package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.input.Input;

import java.io.Closeable;
import java.io.IOException;

public interface ImportLogicInterface extends Closeable{

    //public void initialize( Dependencies dependencies ) throws IOException;

    public void initialize( Input input) throws IOException;
    public void prepareIdMapper();
    public PropertyValueLookup getNodeInputIdLookup();
    public void importNodes() throws IOException;
    public void importRelationships() throws IOException;
    public void calculateNodeDegrees();
    public void buildInternalStructures();
    public void buildCountsStore();
    public void updatePeakMemoryUsage();
    public void success();
    public NumberArrayFactory getNumberArrayFactory();

    //=============
    //public void setImportLogicValues(BatchingStoreInterface store, Input input,
    //                                 NodeRelationshipCache nodeRelationshipCache, IdMapper idMapper, NumberArrayFactory numberArrayFactory);
    //public void defragmentRelationshipGroups();

    //public void logFilesInit(LogFilesInitializer logFilesInitializer, BatchingStoreInterface store);

    //public BatchingStoreInterface getStore();
    //public MemoryUsageStatsProvider getMemoryUsageStats();
    //public Input.Estimates getInputEstimates() throws IOException;
    //public LongFunction<Object> getNodeInputIdLookup();
}