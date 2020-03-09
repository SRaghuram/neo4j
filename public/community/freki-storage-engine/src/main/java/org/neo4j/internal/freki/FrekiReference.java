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

import org.neo4j.storageengine.api.Reference;
import org.neo4j.storageengine.api.RelationshipDirection;

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;

class FrekiReference implements Reference
{
    final long sourceNodeId;
    final boolean relationship;
    final long internalId;
    final int type;
    // should be OUTGOING or LOOP, I'd guess
    final RelationshipDirection direction;
    final long endNodeId;

    private FrekiReference( long sourceNodeId, boolean relationship, long internalId, int type, RelationshipDirection direction, long endNodeId )
    {
        this.sourceNodeId = sourceNodeId;
        this.relationship = relationship;
        this.internalId = internalId;
        this.type = type;
        this.direction = direction;
        this.endNodeId = endNodeId;
    }

    @Override
    public String toString()
    {
        return "FrekiReference{" + "sourceNodeId=" + sourceNodeId + ", relationship=" + relationship + ", internalId=" + internalId + ", type=" + type +
                ", direction=" + direction + ", endNodeId=" + endNodeId + '}';
    }

    static FrekiReference relationshipReference( long sourceNodeId, long internalId, int type, RelationshipDirection direction, long endNodeId )
    {
        return new FrekiReference( sourceNodeId, true, internalId, type, direction, endNodeId );
    }

    static FrekiReference nodeReference( long nodeId )
    {
        return new FrekiReference( nodeId, false, -1, -1, null, -1 );
    }

    static FrekiReference NULL_REFERENCE = nodeReference( NULL );
}
