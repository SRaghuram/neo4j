package org.neo4j.kernel.impl.store.cursors;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.storageengine.api.*;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;

public class MyNodeCursor implements StorageNodeCursor {

    //public static final int NO_ID = -1;
    MyStore myStore;
    long currentNodeId = MyStore.START_ID;
    long[] currentNode = new long[MyStore.NODE_SIZE];
    private PageCursor pageCursor;
    private long next;
    private long highMark;
    private long nextStoreReference;
    private boolean open;
    private boolean batched;

    public MyNodeCursor( MyStore store )
    {
        this.myStore = store;
    }
    @Override
    public long[] labels() {
        int[] labelInt = myStore.getLabels(currentNodeId);
        long[] labelLong = new long[labelInt.length];
        for (int i = 0; i < labelInt.length; i++)
            labelLong[i] = (long)labelInt[i];
        return labelLong;
    }

    @Override
    public boolean hasLabel(int label) {
        int[] labelInt = myStore.getLabels(currentNodeId);
        for (int i = 0; i < labelInt.length; i++)
            if (label == labelInt[i])
                return true;
        return false;
    }

    @Override
    public long relationshipsReference() {
        return 0;
    }

    @Override
    public void relationships(StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection) {

    }

    @Override
    public int[] relationshipTypes() {
        return new int[0];
    }

    @Override
    public Degrees degrees(RelationshipSelection selection) {
        return null;
    }

    @Override
    public boolean supportsFastDegreeLookup() {
        return false;
    }


    public long relationshipGroupReference() {
        return currentNode[3];
    }


    public long allRelationshipsReference() {
        return currentNode[3];
    }


    public boolean isDense() {
        return false;
    }

    @Override
    public void scan() {
        if ( currentNodeId != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( MyStore.START_ID );
        }
        this.next = MyStore.START_ID;
        this.highMark = nodeHighMark();
        this.nextStoreReference = NO_ID;
        this.open = true;
        this.batched = false;
    }

    @Override
    public boolean scanBatch(AllNodeScan scan, int sizeHint) {
        if ( currentNodeId != NO_ID )
        {
            reset();
        }
        this.batched = true;
        this.open = true;
        this.nextStoreReference = NO_ID;

        return ((MyNodeScan) scan).scanBatch( sizeHint , this);
    }

    boolean scanRange( long start, long stop )
    {
        long max = nodeHighMark();
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
            pageCursor = nodePage( start );
        }
        next = start;
        highMark = min( stop, max );
        return true;
    }

    @Override
    public void single(long reference) {
        if ( currentNodeId != NO_ID )
        {
            resetState();
        }
        if ( pageCursor == null )
        {
            pageCursor = nodePage( reference );
        }
        this.next = reference >= 0 ? reference : NO_ID;
        //This marks the cursor as a "single cursor"
        this.highMark = NO_ID;
        this.nextStoreReference = NO_ID;
        this.open = true;
        this.batched = false;
    }

    @Override
    public boolean hasProperties() {
        if (currentNode[2] != NO_ID)
            return true;
        return false;
    }

    @Override
    public long propertiesReference() {
        return getNextProp();
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor) {
        propertyCursor.initNodeProperties( getNextProp() );
    }
    public long getNextProp()
    {
        return currentNode[2];
    }

    @Override
    public long entityReference() {
        return currentNodeId;
    }

    @Override
    public boolean next() {
            if (next == NO_ID) {
                resetState();
                return false;
            }

            do {
                if (nextStoreReference == next) {
                    nodeAdvance( pageCursor );
                    next++;
                    nextStoreReference++;
                } else {
                    node(next++, pageCursor);
                    nextStoreReference = next;
                }

                if (next > highMark) {
                    if (isSingle() || batched) {
                        //we are a "single cursor" or a "batched scan"
                        //we don't want to set a new highMark
                        next = NO_ID;
                        return inUse();
                    } else {
                        //we are a "scan cursor"
                        //Check if there is a new high mark
                        highMark = nodeHighMark();
                        if (next > highMark) {
                            next = NO_ID;
                            return inUse();
                        }
                    }
                }
            }
            while (!inUse());
            return true;
    }

    private void node( long reference, PageCursor pageCursor )
    {
        //read.getRecordByCursor( reference, pageCursor );
        currentNode = myStore.getCell( reference, MyStore.MyStoreType.NODE );
        currentNodeId = reference;
    }

    private void nodeAdvance( PageCursor pageCursor )
    {
        //read.nextRecordByCursor( pageCursor );
        myStore.nextRecordByCursor( pageCursor, MyStore.MyStoreType.NODE );
        node( currentNodeId+1, pageCursor);
    }

    private PageCursor nodePage( long reference )
    {
        //return read.openPageCursorForReading( reference );
        return myStore.openPageCursorForReading( reference, MyStore.MyStoreType.NODE );
    }

    @Override
    public void reset()
    {
        if ( open )
        {
            open = false;
            resetState();
        }
    }

    @Override
    public void close() {

    }
    private long nodeHighMark()
    {
        return myStore.getHighestPossibleIdInUse(MyStore.MyStoreType.NODE);//read.getHighestPossibleIdInUse();
    }
    private void resetState()
    {
        currentNodeId = NO_ID;
        currentNode = null;
        //setId( NO_ID );
        //clear();
    }
    private boolean isSingle()
    {
        return highMark == NO_ID;
    }
    public final boolean inUse()
    {
        //return inUse;
        if (currentNode[0] != -1)
            return true;
        return false;
    }

    abstract class MyBaseScan<MyNodeCursor>
    {
        private final AtomicLong nextStart = new AtomicLong( 0 );

        boolean scanBatch( int sizeHint, MyNodeCursor cursor )
        {
            long start = nextStart.getAndAdd( sizeHint );
            long stopInclusive = start + sizeHint - 1;
            return scanRange( cursor, start, stopInclusive );
        }

        abstract boolean scanRange( MyNodeCursor cursor, long start, long stopInclusive );
    }
    final class MyNodeScan extends MyBaseScan<MyNodeCursor> implements AllNodeScan
    {
        @Override
        boolean scanRange( MyNodeCursor cursor, long start, long stopInclusive )
        {
            return cursor.scanRange( start, stopInclusive );
        }
    }
}
