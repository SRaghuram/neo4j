/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;

/**
 * Description of a lock that was deferred to commit time.
 */
public class LockUnit extends ActiveLock implements Comparable<LockUnit>
{
    public LockUnit( ResourceType resourceType, LockType lockType, long userTransactionId, long resourceId )
    {
        super( resourceType, lockType, userTransactionId, resourceId );
    }

    public boolean isExclusive()
    {
        return LockType.EXCLUSIVE == lockType();
    }

    @Override
    public int compareTo( LockUnit o )
    {
        // Exclusive locks go first to minimize amount of potential deadlocks
        int exclusiveCompare = o.lockType().compareTo( lockType() );
        if ( exclusiveCompare != 0 )
        {
            return exclusiveCompare;
        }

        // Then shared/exclusive locks are compared by resourceTypeId and then by resourceId
        return resourceType().typeId() == o.resourceType().typeId() ? Long.compare( resourceId(), o.resourceId() )
                                                                : Integer.compare( resourceType().typeId(), o.resourceType().typeId() );
    }

    @Override
    public String toString()
    {
        return "Resource [resourceType=" + resourceType() + ", resourceId=" + resourceId() + ", lockType=" + lockType() +
                ", transactionId=" + transactionId() + "]";
    }
}
