package org.neo4j.utils.runutils;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.store.StoreAccess;

public interface IProcessor
{
    void prepare( RecordStore store, StoreAccess storeAccess );
    
    void process( Object record );

    void close();
    
    boolean isParallel();
}
