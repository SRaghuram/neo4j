package org.neo4j.propertystorereorg;

import org.neo4j.utils.PackedMultiFieldCache;

public class NodeIdMapByLabel extends PackedMultiFieldCache
{

    public static NodeIdMapByLabel nodeIdMap = null;
    public NodeIdMapByLabel()
    {
        super( 40 );
        nodeIdMap = this;
    }
    
    public long maxCount()
    {
        return nodeIdMap.maxIndex();
    }
    
    public long get (long index)
    {
        long max = nodeIdMap.maxIndex();
        return nodeIdMap.getFromCache( index )[0];
    }
    
    public void set(long index, long value)
    {
        nodeIdMap.putToCache( index, value );
        if (nodeIdMap.getFromCache(  index )[0] != value)
            System.out.println("error");
    }
    
}
