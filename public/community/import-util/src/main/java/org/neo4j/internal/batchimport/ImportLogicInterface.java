package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.cache.NumberArrayFactory;
import org.neo4j.internal.batchimport.input.Input;

import java.io.Closeable;
import java.io.IOException;

public interface ImportLogicInterface extends Closeable{
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
}
