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

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;

import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;

public class FrekiRelationshipTraversalCursor implements StorageRelationshipTraversalCursor
{
    private final Store mainStore;
    Record record = new Record( 1 );
    private ByteBuffer data;

    private PageCursor cursor;
    boolean loadedCorrectNode;
    private long nodeId;
    private int expectedType;
    private RelationshipDirection expectedDirection;
    private int numRelationships;
    private int currentRelationshipIndex;
    private int currentRelationshipType;
    private RelationshipDirection currentRelationshipDirection;
    private long currentRelationshipOtherNode;

    FrekiRelationshipTraversalCursor( Store mainStore )
    {
        this.mainStore = mainStore;
        reset();
    }

    @Override
    public boolean hasProperties()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long propertiesReference()
    {
        return -1;
    }

    @Override
    public void properties( StoragePropertyCursor propertyCursor )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    @Override
    public long entityReference()
    {
        return 0;
    }

    @Override
    public boolean next()
    {
        if ( !loadedCorrectNode || currentRelationshipIndex != -1 )
        {
            if ( !loadedCorrectNode )
            {
                mainStore.read( cursor(), record, nodeId );
                loadedCorrectNode = true;
                currentRelationshipIndex = 0;
                if ( !record.hasFlag( Record.FLAG_IN_USE ) )
                {
                    return false;
                }
                readHeaderAndPrepareBuffer();
            }

            while ( currentRelationshipIndex < numRelationships )
            {
                currentRelationshipIndex++;

                int relHeader = data.getInt();
                currentRelationshipType = relHeader & 0x7FFFFFFF;
                currentRelationshipOtherNode = data.getLong();
                if ( currentRelationshipOtherNode == nodeId )
                {
                    currentRelationshipDirection = LOOP;
                }
                else
                {
                    boolean outgoing = (relHeader & 0x80000000) != 0;
                    currentRelationshipDirection = outgoing ? OUTGOING : INCOMING;
                }

                boolean matchesType = expectedType == -1 || currentRelationshipType == expectedType;
                boolean matchesDirection = expectedDirection == null || currentRelationshipDirection.equals( expectedDirection );
                if ( matchesType && matchesDirection )
                {
                    return true;
                }
            }
        }
        return false;
    }

    private void readHeaderAndPrepareBuffer()
    {
        // Read property offset
        data = record.dataForReading();
        int offsetsHeader = MutableNodeRecordData.readOffsetsHeader( data );
        int offset = MutableNodeRecordData.relationshipOffset( offsetsHeader );
        data.position( offset );
        numRelationships = (data.get() & 0xFF);
    }

    private PageCursor cursor()
    {
        if ( cursor == null )
        {
            cursor = mainStore.openReadCursor();
        }
        return cursor;
    }

    @Override
    public void reset()
    {
        nodeId = -1;
        expectedType = -1;
        expectedDirection = null;
        numRelationships = -1;
        currentRelationshipIndex = -1;
        loadedCorrectNode = false;
    }

    @Override
    public void close()
    {
        if ( cursor != null )
        {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public int type()
    {
        return currentRelationshipType;
    }

    @Override
    public long sourceNodeReference()
    {
        return originNodeReference();
    }

    @Override
    public long targetNodeReference()
    {
        return neighbourNodeReference();
    }

    @Override
    public long neighbourNodeReference()
    {
        return currentRelationshipDirection.equals( OUTGOING ) ? currentRelationshipOtherNode : nodeId;
    }

    @Override
    public long originNodeReference()
    {
        return currentRelationshipDirection.equals( INCOMING ) ? currentRelationshipOtherNode : nodeId;
    }

    @Override
    public void init( long nodeId, long reference, boolean nodeIsDense )
    {
        reset();
        this.nodeId = nodeId;
    }

    @Override
    public void init( long nodeId, long reference, int type, RelationshipDirection direction, boolean nodeIsDense )
    {
        init( nodeId, reference, nodeIsDense );
        this.expectedType = type;
        this.expectedDirection = direction;
    }
}
