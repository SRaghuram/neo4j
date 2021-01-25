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
package org.neo4j.batchinsert.internal;

import org.neo4j.graphdb.RelationshipType;

/**
 * Simple relationship wrapping start node id, end node id and relationship
 * type.
 */
public final class BatchRelationship
{
    private final long id;
    private final long startNodeId;
    private final long endNodeId;
    private final RelationshipType type;

    public BatchRelationship( long id, long startNodeId, long endNodeId,
        RelationshipType type )
    {
        this.id = id;
        this.startNodeId = startNodeId;
        this.endNodeId = endNodeId;
        this.type = type;
    }

    public long getId()
    {
        return id;
    }

    public long getStartNode()
    {
        return startNodeId;
    }

    public long getEndNode()
    {
        return endNodeId;
    }

    public RelationshipType getType()
    {
        return type;
    }
}
