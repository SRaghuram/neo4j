/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
<<<<<<< HEAD
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.Reference;
=======
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipDirection;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
<<<<<<< HEAD
import org.neo4j.storageengine.util.EagerDegrees;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.internal.freki.StreamVByte.nonEmptyIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
=======

import static java.lang.Math.min;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.addAll;
import static org.neo4j.internal.freki.MutableNodeData.readDegreesForNextType;
import static org.neo4j.internal.freki.StreamVByte.LONG_CONSUMER;
import static org.neo4j.internal.freki.StreamVByte.LONG_CREATOR;
import static org.neo4j.internal.freki.StreamVByte.hasNonEmptyIntArray;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import static org.neo4j.internal.freki.StreamVByte.readInts;
import static org.neo4j.token.api.TokenConstants.ANY_RELATIONSHIP_TYPE;

class FrekiNodeCursor extends FrekiMainStoreCursor implements StorageNodeCursor
{
    private long singleId;
<<<<<<< HEAD
    private boolean inScan;
    private FrekiRelationshipTraversalCursor relationshipsCursor;

    FrekiNodeCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer );
=======
    private long highMark = NULL;
    private boolean inScan;
    private FrekiRelationshipTraversalCursor relationshipsCursor;
    private boolean lightweight;

    FrekiNodeCursor( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        super( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public long[] labels()
    {
        cursorAccessTracer.registerNodeLabelsAccess();
<<<<<<< HEAD
        ByteBuffer buffer = data.labelBuffer();
        return buffer != null ? readIntDeltas( new StreamVByte.LongArrayTarget(), buffer ).array() : EMPTY_LONG_ARRAY;
=======
        long[] labels;
        boolean shouldRetry;
        int failures = 0;
        do
        {
            shouldRetry = false;
            ensureLabelsLocated();
            ByteBuffer buffer = data.labelBuffer();
            if ( buffer == null )
            {
                return EMPTY_LONG_ARRAY;
            }
            labels = (long[]) readInts( buffer, true, LONG_CREATOR, LONG_CONSUMER );

            // === Logic for split label data
            if ( data.labelsSplitState != null )
            {
                data.labelsSplitState.reset();
                do
                {
                    SPLIT_PIECE_LOAD_STATUS status = loadNextSplitPiece( Header.FLAG_LABELS, data.labelsSplitState );
                    if ( status != SPLIT_PIECE_LOAD_STATUS.OK )
                    {
                        if ( failures++ > MAX_RETRIES_BEFORE_TIMER )
                        {
                            throw new IllegalStateException( "Failed to load split label part chain. " + status.name() );
                        }
                        shouldRetry = true;
                        break;
                    }
                    long[] moreLabels = (long[]) readInts( data.labelsSplitState.buffer, true, LONG_CREATOR, LONG_CONSUMER );
                    labels = addAll( labels, moreLabels );
                }
                while ( !data.labelsSplitState.last );
            }
            if ( shouldRetry )
            {
                prepareForReload();
            }
        }
        while ( shouldRetry );
        assert isOrdered( labels );
        return labels;
    }

    private static boolean isOrdered( long[] labels )
    {
        long[] sorted = labels.clone();
        Arrays.sort( sorted );
        return Arrays.equals( labels, sorted );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public boolean hasLabel( int label )
    {
        for ( long labelId : labels() )
        {
            if ( label == labelId )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public long relationshipsReference()
    {
        return data.nodeId;
    }

    @Override
    public void relationships( StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection )
    {
<<<<<<< HEAD
=======
        ensureRelationshipsLocated();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        traversalCursor.init( this, selection );
    }

    @Override
    public boolean relationshipsTo( StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection, long neighbourNodeReference )
    {
<<<<<<< HEAD
=======
        ensureRelationshipsLocated();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        ((FrekiRelationshipTraversalCursor) traversalCursor).init( this, selection, neighbourNodeReference );
        return true;
    }

    @Override
    public int[] relationshipTypes()
    {
        readRelationshipTypes();
<<<<<<< HEAD
        return relationshipTypesInNode.clone();
    }

    @Override
    public Degrees degrees( RelationshipSelection selection )
    {
        // Dense
        if ( data.isDense )
        {
            ByteBuffer buffer = readRelationshipTypes();
            EagerDegrees degrees = new EagerDegrees();
            if ( relationshipTypesInNode.length == 0 )
            {
                return degrees;
            }

            // Read degrees where relationship data would be if this would have been a sparse node
            int[] degreesArray = readInts( new StreamVByte.IntArrayTarget(), buffer ).array();
            for ( int i = 0; i < selection.numberOfCriteria(); i++ )
            {
                RelationshipSelection.Criterion criterion = selection.criterion( i );
                if ( criterion.type() == ANY_RELATIONSHIP_TYPE ) // all types
                {
                    for ( int typeIndex = 0; typeIndex < relationshipTypesInNode.length; typeIndex++ )
                    {
                        readDenseDegreesForType( degrees, degreesArray, typeIndex );
                    }
                }
                else // a single type
                {
                    int typeIndex = Arrays.binarySearch( relationshipTypesInNode, criterion.type() );
                    if ( typeIndex >= 0 )
                    {
                        readDenseDegreesForType( degrees, degreesArray, typeIndex );
                    }
                }
            }
            return degrees;
        }

        // Sparse
        if ( relationshipsCursor == null )
        {
            relationshipsCursor = new FrekiRelationshipTraversalCursor( stores, cursorAccessPatternTracer, cursorTracer );
        }
        EagerDegrees degrees = new EagerDegrees();
        relationshipsCursor.init( this, selection );
        // TODO If the selection is for any direction then this can be made more efficient by simply looking at the vbyte relationship array length
        while ( relationshipsCursor.next() )
        {
            degrees.add( relationshipsCursor.type(), relationshipsCursor.currentDirection(), 1 );
        }
        return degrees;
    }

    private void readDenseDegreesForType( EagerDegrees degrees, int[] degreesArray, int typeIndex )
    {
        int baseIndex = typeIndex * 3;
        degrees.add( relationshipTypesInNode[typeIndex], degreesArray[baseIndex], degreesArray[baseIndex + 1], degreesArray[baseIndex + 2] );
=======
        int[] types = relationshipTypesInNode.clone();
        if ( data.isDense && data.degreesSplitState != null )
        {
            data.degreesSplitState.reset();
            do
            {
                SPLIT_PIECE_LOAD_STATUS status = loadNextSplitPiece( Header.OFFSET_DEGREES, data.degreesSplitState );
                if ( status != SPLIT_PIECE_LOAD_STATUS.OK )
                {
                    throw new IllegalStateException( "Failed to load split degrees part chain (Retry not yet implemented). " + status.name() );
                }
                readRelationshipTypes( data.degreesSplitState.buffer );
                types = addAll( types, relationshipTypesInNode );
            }
            while ( !data.degreesSplitState.last );
        }
        return types;
    }

    @Override
    public void degrees( RelationshipSelection selection, Degrees.Mutator degrees, boolean allowFastDegreeLookup )
    {
        //TODO what do we do about allowFastDegreeLookup?
        // Dense
        ensureRelationshipsLocated();
        if ( data.isDense )
        {
            ByteBuffer buffer = readRelationshipTypes();
            if ( data.degreesSplitState != null )
            {
                data.degreesSplitState.reset();
            }

            while ( buffer != null )
            {
                if ( relationshipTypesInNode.length > 0 )
                {
                    // Read degrees where relationship data would be if this would have been a sparse node
                    int[] degreesArray = readInts( buffer, false );
                    for ( int i = 0; i < selection.numberOfCriteria(); i++ )
                    {
                        int degreesIndex = 0;
                        RelationshipSelection.Criterion criterion = selection.criterion( i );
                        if ( criterion.type() == ANY_RELATIONSHIP_TYPE ) // all types
                        {
                            for ( int type : relationshipTypesInNode )
                            {
                                degreesIndex = readDegreesForNextType( degrees, type, criterion.direction(), degreesArray, degreesIndex );
                            }
                        }
                        else // a single type
                        {
                            int typeIndex = Arrays.binarySearch( relationshipTypesInNode, criterion.type() );
                            if ( typeIndex >= 0 )
                            {
                                for ( int j = 0; j < typeIndex; j++ ) //fastforward to correct index
                                {
                                    degreesIndex += (degreesArray[degreesIndex] & 0x1) != 0 ? 3 : 2; //loop or not
                                }
                                readDegreesForNextType( degrees, relationshipTypesInNode[typeIndex], criterion.direction(), degreesArray, degreesIndex );
                            }
                        }
                    }
                }
                if ( data.degreesSplitState != null && !data.degreesSplitState.last )
                {
                    SPLIT_PIECE_LOAD_STATUS status = loadNextSplitPiece( Header.OFFSET_DEGREES, data.degreesSplitState );
                    if ( status != SPLIT_PIECE_LOAD_STATUS.OK )
                    {
                        throw new IllegalStateException( "Failed to load split degrees part chain (Retry not yet implemented). " + status.name() );
                    }
                    buffer = data.degreesSplitState.buffer;
                    readRelationshipTypes( buffer );
                }
                else
                {
                    buffer = null;
                }
            }
        }
        else
        {
            // Sparse
            if ( relationshipsCursor == null )
            {
                relationshipsCursor = new FrekiRelationshipTraversalCursor( stores, cursorAccessPatternTracer, cursorTracer, memoryTracker );
            }
            relationshipsCursor.init( this, selection );
            // TODO If the selection is for any direction then this can be made more efficient by simply looking at the vbyte relationship array length
            while ( relationshipsCursor.next() )
            {
                int outgoing = 0;
                int incoming = 0;
                int loop = 0;
                RelationshipDirection direction = relationshipsCursor.currentDirection();
                if ( direction == RelationshipDirection.OUTGOING )
                {
                    outgoing++;
                }
                else if ( direction == RelationshipDirection.INCOMING )
                {
                    incoming++;
                }
                else
                {
                    loop++;
                }
                degrees.add( relationshipsCursor.type(), outgoing, incoming, loop );
            }
        }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public boolean supportsFastDegreeLookup()
    {
        return data.isDense;
    }

    @Override
    public void scan()
    {
<<<<<<< HEAD
        inScan = true;
        singleId = 0;
=======
        scan( true );
    }

    public void scan( boolean lightweight )
    {
        this.lightweight = lightweight;
        inScan = true;
        singleId = 0;
        highMark = NULL;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public boolean scanBatch( AllNodeScan scan, int sizeHint )
    {
<<<<<<< HEAD
        throw new UnsupportedOperationException( "Not implemented yet" );
=======
        if ( singleId != NULL )
        {
            reset();
        }
        inScan = true;
        singleId = NULL;
        highMark = NULL;
        return ((FrekiNodeScan) scan).scanBatch( sizeHint, this );
    }

    boolean scanRange( long start, long stop )
    {
        long max = highMark();
        if ( start >= max )
        {
            reset();
            return false;
        }
        if ( start >= stop )
        {
            reset();
            return true;
        }
        inScan = true;
        singleId = start;
        highMark = min( stop, max );
        return true;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public void single( long reference )
    {
<<<<<<< HEAD
        singleId = reference;
=======
        single( reference, null );
    }

    void single( long reference, RecordLookup additionalRecordLookup )
    {
        singleId = reference;
        this.additionalRecordLookup = additionalRecordLookup;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public boolean hasProperties()
    {
<<<<<<< HEAD
        ensurePropertiesLoaded();
        ByteBuffer buffer = data.propertyBuffer();
        return buffer != null && nonEmptyIntDeltas( buffer.array(), buffer.position() );
=======
        ensurePropertiesLocated();
        ByteBuffer buffer = data.propertyBuffer();
        return buffer != null && hasNonEmptyIntArray( buffer );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    }

    @Override
    public Reference propertiesReference()
    {
        return FrekiReference.nodeReference( entityReference() );
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        propertyCursor.initNodeProperties( this );
    }

    @Override
    public long entityReference()
    {
        return data.nodeId;
    }

    @Override
    public boolean next()
    {
        if ( inScan )
        {
<<<<<<< HEAD
            while ( singleId < stores.mainStore.getHighId() )
            {
                if ( load( singleId++ ) )
=======
            while ( singleId < highMark() )
            {
                boolean loaded = lightweight ? loadSuperLight( singleId ) : load( singleId );
                singleId++;
                if ( loaded )
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                {
                    return true;
                }
            }
            inScan = false;
            singleId = NULL;
<<<<<<< HEAD
=======
            highMark = NULL;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }
        else if ( singleId != NULL )
        {
            boolean loaded = load( singleId );
            singleId = NULL;
            return loaded;
        }
        return false;
    }

    @Override
    public void reset()
    {
        super.reset();
        singleId = NULL;
<<<<<<< HEAD
=======
        highMark = NULL;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        inScan = false;
        if ( relationshipsCursor != null )
        {
            relationshipsCursor.reset();
        }
    }

    @Override
    public void close()
    {
        if ( relationshipsCursor != null )
        {
            relationshipsCursor.close();
            relationshipsCursor = null;
        }
        super.close();
    }
<<<<<<< HEAD
=======

    private long highMark()
    {
        return highMark != NULL ? highMark : stores.mainStore.getHighId();
    }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
}
