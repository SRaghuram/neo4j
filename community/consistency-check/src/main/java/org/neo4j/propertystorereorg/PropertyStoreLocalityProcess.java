package org.neo4j.propertystorereorg;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.store.StoreAccess;
import org.neo4j.store.StoreAccess.AccessStats;
import org.neo4j.utils.ToolUtils;
import org.neo4j.utils.runutils.RecordProcessor;
public class PropertyStoreLocalityProcess implements RecordProcessor<Abstract64BitRecord> {

	PropertyStore store;
	StoreAccess storeAccess;
	private static long prevStrId = -1, prevArrayId = -1;
	public PropertyStoreLocalityProcess( PropertyStore store, StoreAccess storeAccess ) {
		this.store = store;
		this.storeAccess = storeAccess;
		storeAccess.resetStats( 500 );
	}

	@Override
    public void process( Abstract64BitRecord record )
    {
	    AccessStats accessStats = storeAccess.getAccessStats(store);
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

    @Override
    public void close()
    {
        ToolUtils.saveMessage(ToolUtils.getMaxIds(storeAccess.getRawNeoStore()));
        ToolUtils.saveMessage( storeAccess.getAccessStatsStr() );
        ToolUtils.printSavedMessage(false);
    }

}