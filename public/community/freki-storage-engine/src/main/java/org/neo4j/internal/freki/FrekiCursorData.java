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

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.MutableNodeRecordData.forwardPointerToString;

/**
 * Data that cursors need to read data. This is a minimal parsed version of data loaded to a {@link Record} from a {@link Store}.
 * Different types of data can exist in different records, so those groups come in pairs: [offset, buffer] where a buffer
 * can be shared with other pairs if e.g. labels and properties happens to live in the same buffer (i.e. same record).
 */
class FrekiCursorData
{
    Record[] records;

    long nodeId = NULL;
    boolean x1Loaded;
    long forwardPointer = NULL;
    boolean xLLoaded;
    long backwardPointer = NULL;

    int labelOffset;
    private ByteBuffer labelBuffer;
    int propertyOffset;
    private ByteBuffer propertyBuffer;
    int relationshipOffset;
    private ByteBuffer relationshipBuffer;
    int endOffset;

    int refCount = 1;

    FrekiCursorData( int numMainStores )
    {
        this.records = new Record[numMainStores];
    }

    void assignLabelOffset( int offset, ByteBuffer buffer )
    {
        if ( offset > 0 )
        {
            labelOffset = offset;
            labelBuffer = buffer;
        }
    }

    void assignPropertyOffset( int offset, ByteBuffer buffer )
    {
        if ( offset > 0 )
        {
            propertyOffset = offset;
            propertyBuffer = buffer;
        }
    }

    void assignRelationshipOffset( int offset, ByteBuffer buffer )
    {
        if ( offset > 0 )
        {
            relationshipOffset = offset;
            relationshipBuffer = buffer;
        }
    }

    ByteBuffer labelBuffer()
    {
        return labelOffset == 0 ? null : labelBuffer.position( labelOffset );
    }

    ByteBuffer labelBuffer( int offset )
    {
        return labelBuffer.position( offset );
    }

    ByteBuffer propertyBuffer()
    {
        return propertyOffset == 0 ? null : propertyBuffer.position( propertyOffset );
    }

    ByteBuffer propertyBuffer( int offset )
    {
        return propertyBuffer.position( offset );
    }

    ByteBuffer relationshipBuffer()
    {
        return relationshipOffset == 0 ? null : relationshipBuffer.position( relationshipOffset );
    }

    ByteBuffer relationshipBuffer( int offset )
    {
        return relationshipBuffer.position( offset );
    }

    boolean isLoaded()
    {
        return x1Loaded || xLLoaded;
    }

    void reset()
    {
        assert refCount == 1;
        nodeId = NULL;
        x1Loaded = false;
        forwardPointer = NULL;
        xLLoaded = false;
        backwardPointer = NULL;
        labelOffset = 0;
        propertyOffset = 0;
        relationshipOffset = 0;
        endOffset = 0;
    }

    @Override
    public String toString()
    {
        return isLoaded() ? String.format( "NodeData[%d,fw:%s,lo:%d,po:%d,ro:%d,eo:%d]", nodeId, forwardPointerToString( forwardPointer ), labelOffset,
                                propertyOffset, relationshipOffset, endOffset )
                          : "<not loaded>";
    }
}
