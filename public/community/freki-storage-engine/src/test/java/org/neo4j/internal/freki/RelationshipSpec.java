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

import java.util.Objects;
import java.util.Set;

import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

class RelationshipSpec
{
    final long id;
    final long startNodeId;
    final int type;
    final long endNodeId;
    final Set<StorageProperty> properties;

    RelationshipSpec( long startNodeId, int type, long endNodeId, Set<StorageProperty> properties, CommandCreationContext commandCreationContext )
    {
        this( startNodeId, type, endNodeId, properties, commandCreationContext.reserveRelationship( startNodeId ) );
    }

    RelationshipSpec( long startNodeId, int type, long endNodeId, Set<StorageProperty> properties, long id )
    {
        this.startNodeId = startNodeId;
        this.type = type;
        this.endNodeId = endNodeId;
        this.properties = properties;
        this.id = id;
    }

    RelationshipDirection direction( long fromPovOfNodeId )
    {
        return startNodeId == fromPovOfNodeId ? endNodeId == fromPovOfNodeId ? RelationshipDirection.LOOP : RelationshipDirection.OUTGOING
                                              : RelationshipDirection.INCOMING;
    }

    @Override
    public String toString()
    {
        return "RelationshipSpec{" + "startNodeId=" + startNodeId + ", type=" + type + ", endNodeId=" + endNodeId + ", properties=" + properties + ", id=" +
                id + '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        RelationshipSpec that = (RelationshipSpec) o;
        return startNodeId == that.startNodeId && type == that.type && endNodeId == that.endNodeId && Objects.equals( properties, that.properties ) &&
                id == that.id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( id, startNodeId, type, endNodeId, properties );
    }

    void create( TxStateVisitor target )
    {
        try
        {
            target.visitCreatedRelationship( id, type, startNodeId, endNodeId, properties );
        }
        catch ( ConstraintValidationException e )
        {
            throw new RuntimeException( e );
        }
    }

    long neighbourNode( long fromNodeIdPov )
    {
        return startNodeId == fromNodeIdPov ? endNodeId : startNodeId;
    }
}
