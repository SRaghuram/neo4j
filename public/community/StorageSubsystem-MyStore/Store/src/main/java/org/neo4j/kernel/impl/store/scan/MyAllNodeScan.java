package org.neo4j.kernel.impl.store.scan;

import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.kernel.impl.store.cursors.MyNodeCursor;
import org.neo4j.storageengine.api.AllNodeScan;

import java.util.concurrent.atomic.AtomicLong;

public class MyAllNodeScan implements AllNodeScan {

    private final AtomicLong nextStart = new AtomicLong( 0 );
    MyStore store;
    public MyAllNodeScan( MyStore store )
    {
        this.store = store;
    }
    boolean scanBatch( int sizeHint, MyNodeCursor cursor )
    {
        long start = nextStart.getAndAdd( sizeHint );
        long stopInclusive = start + sizeHint - 1;
        return scanRange( cursor, start, stopInclusive );
    }
    boolean scanRange(MyNodeCursor cursor, long start, long stopInclusive )
    {
        return false;
    }
}
