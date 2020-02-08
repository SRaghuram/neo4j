package org.neo4j.kernel.impl.store.scan;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.kernel.impl.store.cursors.MyRelationshipCursor;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;

public class MyRelationshipScanCursor extends MyRelationshipCursor implements StorageRelationshipScanCursor {

    private int filterType;
    private long next;
    private long highMark;
    private long nextStoreReference;
    private PageCursor pageCursor;
    private boolean open;
    private boolean batched;
    public MyRelationshipScanCursor(MyStore store) {
        super(store);
    }

    @Override
    public void scan(int type) {
        if ( currentRelationshipId != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = relationshipPage( 0 );
        }
        this.next = 0;
        this.filterType = type;
        this.highMark = relationshipHighMark();
        this.nextStoreReference = NO_ID;
        this.open = true;
    }

    private void resetState()
    {
        currentRelationshipId = next = NO_ID ;
    }
    private long relationshipHighMark()
    {
        return myStore.getHighestPossibleIdInUse(MyStore.MyStoreType.RELATIONSHIP);//read.getHighestPossibleIdInUse();
    }

    @Override
    public void scan() {
        scan( -1 );
    }

    @Override
    public boolean scanBatch(AllRelationshipsScan scan, int sizeHint) {
        if ( currentRelationshipId != NO_ID )
        {
            reset();
        }
        this.batched = true;
        this.open = true;
        this.nextStoreReference = NO_ID;
        this.filterType = -1;

        return ((MyRelationshipScan) scan).scanBatch( sizeHint , this);
    }

    boolean scanRange( long start, long stop )
    {
        long max = relationshipHighMark();
        if ( start > max )
        {
            reset();
            return false;
        }
        if ( start > stop )
        {
            reset();
            return true;
        }
        if ( pageCursor == null )
        {
            pageCursor = relationshipPage( start );
        }
        next = start;
        highMark = min( stop, max );
        return true;
    }

    @Override
    public void single(long reference) {
        if ( currentRelationshipId != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = relationshipPage( reference );
        }
        this.next = reference >= 0 ? reference : NO_ID;
        this.filterType = -1;
        this.highMark = NO_ID;
        this.nextStoreReference = NO_ID;
        this.open = true;
    }


    @Override
    public boolean next() {
        if ( next == NO_ID )
        {
            resetState();
            return false;
        }

        do
        {
            if ( nextStoreReference == next )
            {
                relationshipAdvance( pageCursor );
                next++;
                nextStoreReference++;
            }
            else
            {
                relationship( next++, pageCursor );
                nextStoreReference = next;
            }

            if ( next > highMark )
            {
                if ( isSingle() || batched )
                {
                    //we are a "single cursor" or a "batched scan"
                    //we don't want to set a new highMark
                    next = NO_ID;
                    return inUse();
                }
                else
                {
                    //we are a "scan cursor"
                    //Check if there is a new high mark
                    highMark = relationshipHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                        return isWantedTypeAndInUse();
                    }
                }
            }
        }
        while ( !isWantedTypeAndInUse() );
        return true;
    }

    private boolean isSingle()
    {
        return highMark == NO_ID;
    }
    private boolean isWantedTypeAndInUse()
    {
        return (filterType == -1 || type() == filterType) && inUse();
    }

    @Override
    public void reset() {

    }

    @Override
    public void close() {

    }

    private void relationshipAdvance( PageCursor pageCursor )
    {
        //read.nextRecordByCursor( pageCursor );
        myStore.nextRecordByCursor( pageCursor, MyStore.MyStoreType.RELATIONSHIP );
        relationship(next, pageCursor);
    }

    private PageCursor relationshipPage(long reference )
    {
        //return read.openPageCursorForReading( reference );
        return myStore.openPageCursorForReading( reference, MyStore.MyStoreType.RELATIONSHIP );
    }

    abstract class MyBaseScan<MyRelationshipCursor>
    {
        private final AtomicLong nextStart = new AtomicLong( 0 );

        boolean scanBatch( int sizeHint, MyRelationshipCursor cursor )
        {
            long start = nextStart.getAndAdd( sizeHint );
            long stopInclusive = start + sizeHint - 1;
            return scanRange( cursor, start, stopInclusive );
        }

        abstract boolean scanRange( MyRelationshipCursor cursor, long start, long stopInclusive );
    }
    final class MyRelationshipScan extends MyBaseScan<MyRelationshipScanCursor> implements AllRelationshipsScan
    {
        @Override
        boolean scanRange( MyRelationshipScanCursor cursor, long start, long stopInclusive )
        {
            return cursor.scanRange( start, stopInclusive );
        }
    }
}
