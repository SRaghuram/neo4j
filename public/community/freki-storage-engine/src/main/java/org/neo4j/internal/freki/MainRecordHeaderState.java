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

class MainRecordHeaderState
{
    int labelsOffset;
    int nodePropertiesOffset;
    int relationshipsOffset;
    int endOffset;
    boolean containsForwardPointer;
    boolean containsBackPointer;
    long forwardPointer;
    boolean isDense;
    long backPointer;

    void reset()
    {
        isDense = false;
        containsForwardPointer = false;
        containsBackPointer = false;
    }

    void initializeForSmallRecord( int labelsOffset )
    {
        reset();
        this.labelsOffset = labelsOffset;
    }

    void initialize( int offsetsHeader )
    {
        relationshipsOffset = MutableNodeRecordData.relationshipOffset( offsetsHeader );
        nodePropertiesOffset = MutableNodeRecordData.propertyOffset( offsetsHeader );
        endOffset = MutableNodeRecordData.endOffset( offsetsHeader );
    }

    @Override
    public String toString()
    {
        return "MainRecordHeaderState{" + "labelsOffset=" + labelsOffset + ", nodePropertiesOffset=" + nodePropertiesOffset + ", relationshipsOffset=" +
                relationshipsOffset + ", endOffset=" + endOffset + ", containsForwardPointer=" + containsForwardPointer + ", containsBackPointer=" +
                containsBackPointer + ", forwardPointer=" + forwardPointer + ", isDense=" + isDense + ", backPointer=" + backPointer + '}';
    }
}
