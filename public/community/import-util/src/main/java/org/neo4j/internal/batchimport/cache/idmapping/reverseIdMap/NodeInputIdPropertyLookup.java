package org.neo4j.internal.batchimport.cache.idmapping.reverseIdMap;

import org.neo4j.internal.batchimport.PropertyValueLookup;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.File;
import java.io.IOException;

public class NodeInputIdPropertyLookup implements PropertyValueLookup {
    BaseSimpleStore stringDynamicStore;
    public NodeInputIdPropertyLookup(File tempDirectory,
                                     PageCache pageCache)
    {
        this.stringDynamicStore = new BaseSimpleStore(tempDirectory, pageCache);
    }

    public void save(long nodeId, Object object, PageCursorTracer cursorTracer) {
        try {
            stringDynamicStore.updateRecord(nodeId, (String) object, cursorTracer);
        } catch (IOException io)
            {
                System.out.println(("ERRORRRR:"+ io.getMessage()));
            }
    }

    @Override
    public Object lookupProperty( long nodeId, PageCursorTracer cursorTracer ) {
        byte[] data = new byte[0];
        try {
            data = stringDynamicStore.getRecord( nodeId );
        }
        catch (IOException io)
        {
            System.out.println(("ERROR:"+ io.getMessage()));
        }
        return new String(data);
    }

    public void close()
    {
        this.stringDynamicStore.close();
    }
}
