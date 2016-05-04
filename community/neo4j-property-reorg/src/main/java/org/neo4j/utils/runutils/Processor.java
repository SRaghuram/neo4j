package org.neo4j.utils.runutils;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.store.StoreAccess;
import org.neo4j.store.StoreAccess.AccessStats;

public enum Processor implements IProcessor
{
    CC_GET_STATS
    {
        AccessStats accessStats;
        @Override
        public void prepare(RecordStore store, StoreAccess storeAccess)
        {
            this.store = store;
            this.storeAccess = storeAccess;
            AccessStats accessStats = storeAccess.getAccessStats((CommonAbstractStore)store);
        }
        
        @Override
        public void process( Object record )
        {
            long prevStrId = -1, prevArrayId = -1;

            {
                
                if (record instanceof PropertyRecord)
                {
                    PropertyRecord prop = (PropertyRecord)record;
                    PropertyStore pStore = (PropertyStore)store;
                    if (prop.inUse())
                    {
                        accessStats.upInUse();
                        //return;
                    }
                    accessStats.incrementRREstimate(prop.getId(), prop.getNextProp());
                    for ( PropertyBlock block : (PropertyRecord)record )
                    {
                        PropertyType type = block.forceGetType();
                        if ( type != null )             
                        {
                            long id = block.getSingleValueLong();
                            switch ( type )
                            {
                            case STRING:            
                                 storeAccess.getAccessStats(pStore.getStringStore()).incrementRREstimate(id, prevStrId);
                                 prevStrId = id;
                                 break;
                            case ARRAY:
                                storeAccess.getAccessStats(pStore.getArrayStore()).incrementRREstimate(id, prevArrayId);
                                prevArrayId = id;
                                break;
                            }
                        }
                    }
                    
                }
            }
            
        }

        @Override
        public void close()
        {
            // TODO Auto-generated method stub
            
        }

        @Override
        public boolean isParallel()
        {
            // TODO Auto-generated method stub
            return false;
        }
        
    },
    
    CC_REORG_PROPERTYSTORE
    {
        @Override
        public void process( Object record )
        {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void close()
        {
            // TODO Auto-generated method stub
            
        }

        @Override
        public boolean isParallel()
        {
            // TODO Auto-generated method stub
            return false;
        }
        
    };
    
    RecordStore store; 
    StoreAccess storeAccess;
    @Override
    public void prepare(RecordStore store, StoreAccess storeAccess)
    {
        this.store = store;
        this.storeAccess = storeAccess;
    }
}
