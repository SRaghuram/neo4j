package org.neo4j.kernel.impl.store.cursors;

import org.neo4j.internal.recordstorage.RelationshipReferenceEncoding;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.neo4j.storageengine.api.RelationshipDirection.directionOfStrict;
import static org.neo4j.storageengine.api.StorageEntityScanCursor.NO_ID;

public class MyRelationshipTraversalCursor extends MyRelationshipCursor implements StorageRelationshipTraversalCursor {
    private boolean open;
    private long originNodeReference;
    private GroupState groupState;
    private long next;
    private Record buffer;
    private int filterType = NO_ID;
    private RelationshipDirection filterDirection;
    private boolean lazyFilterInitialization;
    private RelationshipSelection selection;
    public MyRelationshipTraversalCursor(MyStore store) {
        super(store);
    }

    @Override
    public long neighbourNodeReference() {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            return target;
        }
        else if ( target == originNodeReference )
        {
            return source;
        }
        else
        {
            throw new IllegalStateException( "NOT PART OF CHAIN" );
        }
    }

    @Override
    public long originNodeReference() {
        return originNodeReference;
    }

    @Override
    public void init( long nodeReference, long reference, RelationshipSelection selection )
    {
        if ( reference == NO_ID )
        {
            resetState();
            return;
        }

        RelationshipReferenceEncoding encoding = RelationshipReferenceEncoding.parseEncoding( reference );
        reference = RelationshipReferenceEncoding.clearEncoding( reference );

        init( nodeReference, reference, encoding == RelationshipReferenceEncoding.DENSE, selection );
    }

    private void init( long nodeReference, long reference, boolean isDense, RelationshipSelection selection )
    {
        if ( reference == NO_ID )
        {
            resetState();
            return;
        }

        this.selection = selection;
        if ( isDense )
        {
            // The reference points to a relationship group record
            groups( nodeReference, reference );
        }
        else
        {
            // The reference points to a relationship record
            chain( nodeReference, reference );
        }
        open = true;
    }


    private void chain( long nodeReference, long reference )
    {
        if ( pageCursor == null )
        {
            pageCursor = relationshipPage( reference );
        }
        currentRelationshipId = NO_ID ;
        this.groupState = GroupState.NONE;
        this.originNodeReference = nodeReference;
        this.next = reference;
    }

    /*
     * Reference to a group record. Traversal returns mixed types and directions.
     */
    private void groups( long nodeReference, long groupReference )
    {
        currentRelationshipId = NO_ID ;
        this.next = NO_ID;
        this.groupState = GroupState.INCOMING;
        this.originNodeReference = nodeReference;
        //this.group.direct( nodeReference, groupReference );
    }

    @Override
    public boolean next()
    {
        if ( hasBufferedData() )
        {   // We have buffered data, iterate the chain of buffered records
            return nextBuffered();
        }

        do
        {
            boolean traversingDenseNode = false;//traversingDenseNode();
            //if ( traversingDenseNode )
            //{
            //    traverseDenseNode();
            //}

            if ( next == NO_ID )
            {
                resetState();
                return false;
            }

            relationshipFull( next, pageCursor );
            if ( !traversingDenseNode )
            {
                if ( lazyFilterInitialization )
                {
                    filterType = getType();
                    filterDirection = directionOfStrict( originNodeReference, sourceNodeReference(), targetNodeReference() );
                    lazyFilterInitialization = false;
                }
            }
            computeNext();
        }
        while ( !inUse() || (filterType != NO_ID && !correctTypeAndDirection()) );

        //System.out.println("Cur.Relationship:[" + currentRelationshipId+"]"+currentRelationship);
        return true;
    }
    private void computeNext()
    {
        final long source = sourceNodeReference(), target = targetNodeReference();
        if ( source == originNodeReference )
        {
            next = getFirstNextRel();
        }
        else if ( target == originNodeReference )
        {
            next = getSecondNextRel();
        }
        else
        {
            throw new IllegalStateException( "NOT PART OF CHAIN! " + this );
        }
    }
    private boolean correctTypeAndDirection()
    {
        return filterType == getType() && directionOfStrict( originNodeReference, sourceNodeReference(), targetNodeReference() ) == filterDirection;
    }
    private boolean hasBufferedData()
    {
        return buffer != null;
    }
    private boolean nextBuffered()
    {
        buffer = buffer.next;
        if ( !hasBufferedData() )
        {
            resetState();
            return false;
        }
        else
        {
            // Copy buffer data to self
            copyFromBuffer();
        }

        return true;
    }
    private void copyFromBuffer()
    {
       currentRelationship = buffer.cellValue;
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

    private void resetState()
    {
        currentRelationshipId = next = NO_ID;
        groupState = GroupState.NONE;
        filterType = NO_ID;
        filterDirection = null;
        lazyFilterInitialization = false;
        buffer = null;
    }
    static class Record
    {
        final long[] cellValue;
        final Record next;

        /*
         * Initialize the record chain or push a new record as the new head of the record chain
         */
        Record( long[] cell, Record next )
        {
            if ( cell != null )
            {
                cellValue = cell;
            }
            else
            {
                cellValue = null;
            }
            this.next = next;
        }
    }
}
