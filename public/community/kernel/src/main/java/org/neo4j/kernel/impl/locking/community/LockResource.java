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
package org.neo4j.kernel.impl.locking.community;

import java.util.Objects;

import org.neo4j.internal.helpers.MathUtil;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;

public class LockResource
{
    private final ResourceType resourceType;
    private final LockType lockType;
    private final long resourceId;

    /** Local reference count, used for each client to count references to a lock. */
    private int refCount = 1;

    public LockResource( ResourceType resourceType, LockType lockType, long resourceId )
    {
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.lockType = lockType;
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
        LockResource that = (LockResource) o;
        return resourceId == that.resourceId && refCount == that.refCount && resourceType.equals( that.resourceType ) && lockType == that.lockType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( resourceType, resourceId, lockType, refCount );
    }

    @Override
    public String toString()
    {
        return String.format( "%s %s(%d)", resourceType, lockType, resourceId );
    }

    public void acquireReference()
    {
        refCount = Math.incrementExact( refCount );
    }

    public int releaseReference()
    {
        return refCount = MathUtil.decrementExactNotPastZero( refCount );
    }

    public long resourceId()
    {
        return resourceId;
    }

    public ResourceType resourceType()
    {
        return resourceType;
    }

    public LockType getLockType()
    {
        return lockType;
    }
}
