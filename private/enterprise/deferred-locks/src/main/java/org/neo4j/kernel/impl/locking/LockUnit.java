/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.locking;

import org.neo4j.lock.ResourceType;

/**
 * Description of a lock that was deferred to commit time.
 */
public class LockUnit implements Comparable<LockUnit>, ActiveLock
{
    private final ResourceType resourceType;
    private final long resourceId;
    private final boolean exclusive;

    public LockUnit( ResourceType resourceType, long resourceId, boolean exclusive )
    {
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.exclusive = exclusive;
    }

    @Override
    public String mode()
    {
        return exclusive ? EXCLUSIVE_MODE : SHARED_MODE;
    }

    @Override
    public ResourceType resourceType()
    {
        return resourceType;
    }

    @Override
    public long resourceId()
    {
        return resourceId;
    }

    public boolean isExclusive()
    {
        return exclusive;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (exclusive ? 1231 : 1237);
        result = prime * result + (int) (resourceId ^ (resourceId >>> 32));
        result = prime * result + resourceType.hashCode();
        return result;
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj == null )
        {
            return false;
        }
        if ( getClass() != obj.getClass() )
        {
            return false;
        }
        LockUnit other = (LockUnit) obj;
        if ( exclusive != other.exclusive )
        {
            return false;
        }
        if ( resourceId != other.resourceId )
        {
            return false;
        }
        else if ( resourceType.typeId() != other.resourceType.typeId() )
        {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo( LockUnit o )
    {
        // Exclusive locks go first to minimize amount of potential deadlocks
        int exclusiveCompare = Boolean.compare( o.exclusive, exclusive );
        if ( exclusiveCompare != 0 )
        {
            return exclusiveCompare;
        }

        // Then shared/exclusive locks are compared by resourceTypeId and then by resourceId
        return resourceType.typeId() == o.resourceType.typeId() ? Long.compare( resourceId, o.resourceId )
                                                                : Integer.compare( resourceType.typeId(), o.resourceType.typeId() );
    }

    @Override
    public String toString()
    {
        return "Resource [resourceType=" + resourceType + ", resourceId=" + resourceId + ", exclusive=" + exclusive
               + "]";
    }
}
