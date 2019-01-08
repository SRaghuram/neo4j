/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.constraints;

/**
 * Base class describing property constraint on relationships.
 */
public abstract class RelationshipPropertyConstraint extends PropertyConstraint
{
    protected final int relationshipTypeId;

    public RelationshipPropertyConstraint( int relationshipTypeId, int propertyKeyId )
    {
        super( propertyKeyId );
        this.relationshipTypeId = relationshipTypeId;
    }

    public final int relationshipType()
    {
        return relationshipTypeId;
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
        RelationshipPropertyConstraint that = (RelationshipPropertyConstraint) o;
        return propertyKeyId == that.propertyKeyId && relationshipTypeId == that.relationshipTypeId;

    }

    @Override
    public int hashCode()
    {
        return 31 * propertyKeyId + relationshipTypeId;
    }
}
