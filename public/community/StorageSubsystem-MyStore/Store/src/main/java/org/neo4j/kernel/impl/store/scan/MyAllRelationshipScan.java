package org.neo4j.kernel.impl.store.scan;

import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.kernel.impl.store.cursors.MyRelationshipCursor;
import org.neo4j.storageengine.api.AllRelationshipsScan;

import java.util.concurrent.atomic.AtomicLong;

public class MyAllRelationshipScan implements AllRelationshipsScan {

    private final AtomicLong nextStart = new AtomicLong( 0 );
    MyStore store;
    public MyAllRelationshipScan( MyStore store )
    {
        this.store = store;
    }
    boolean scanBatch(int sizeHint, MyRelationshipCursor cursor )
    {
        long start = nextStart.getAndAdd( sizeHint );
        long stopInclusive = start + sizeHint - 1;
        return scanRange( cursor, start, stopInclusive );
    }
    boolean scanRange(MyRelationshipCursor cursor, long start, long stopInclusive )
    {
        return false;
    }
}
