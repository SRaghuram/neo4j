package org.neo4j.propertystorereorg;

import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.propertystorereorg.PropertystoreReorgTool.StoreDetails;
import org.neo4j.store.StoreAccess;
import org.neo4j.store.StoreAccess.AccessStats;
import org.neo4j.utils.ToolUtils;
import org.neo4j.utils.runutils.RecordProcessor;
public class PropertyStoreLocalityProcess implements RecordProcessor<Abstract64BitRecord> {

	PropertyStore store;
	private static long prevStrId = -1, prevArrayId = -1;
	public PropertyStoreLocalityProcess( PropertyStore store, StoreAccess storeAccess ) {
		this.store = store;
		storeAccess.resetStats( 500 );
	}

	@Override
    public void process( Abstract64BitRecord record, StoreDetails storeDetails )
    {
	    AccessStats accessStats = storeDetails.storeAccess.getAccessStats(store);
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
                    	storeDetails.storeAccess.getAccessStats(pStore.getStringStore()).incrementRREstimate(id, prevStrId);
                         prevStrId = id;
                         break;
                    case ARRAY:
                    	storeDetails.storeAccess.getAccessStats(pStore.getArrayStore()).incrementRREstimate(id, prevArrayId);
                        prevArrayId = id;
                        break;
                    }
                }
            }
            
        }
    }

    @Override
    public void close(StoreDetails storeDetails)
    {
        ToolUtils.saveMessage(ToolUtils.getMaxIds(storeDetails.storeAccess.getRawNeoStores()));
        ToolUtils.saveMessage( storeDetails.storeAccess.getAccessStatsStr() );
        ToolUtils.printSavedMessage(false);
    }

}