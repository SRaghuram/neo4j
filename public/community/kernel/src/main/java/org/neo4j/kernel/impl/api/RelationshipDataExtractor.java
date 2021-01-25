/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.storageengine.api.RelationshipVisitor;

public class RelationshipDataExtractor implements RelationshipVisitor<RuntimeException>
{
    private int type;
    private long startNode;
    private long endNode;
    private long relId;

    @Override
    public void visit( long relId, int type, long startNode, long endNode )
    {
        this.relId = relId;
        this.type = type;
        this.startNode = startNode;
        this.endNode = endNode;
    }

    public int type()
    {
        return type;
    }

    public long startNode()
    {
        return startNode;
    }

    public long endNode()
    {
        return endNode;
    }

    public long otherNode( long node )
    {
        if ( node == startNode )
        {
            return endNode;
        }
        else if ( node == endNode )
        {
            return startNode;
        }
        else
        {
            throw new IllegalArgumentException(
                    "Node[" + node + "] is neither start nor end node of relationship[" + relId + "]" );
        }
    }

    public long relationship()
    {
        return relId;
    }
}
