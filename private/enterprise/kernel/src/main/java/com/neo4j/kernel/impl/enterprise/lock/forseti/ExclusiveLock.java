/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.lock.forseti;

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.util.Set;

import org.neo4j.kernel.impl.util.collection.SimpleBitSet;
import org.neo4j.lock.LockType;

import static org.neo4j.lock.LockType.EXCLUSIVE;

class ExclusiveLock implements ForsetiLockManager.Lock
{
    private final ForsetiClient owner;

    ExclusiveLock( ForsetiClient owner )
    {
        this.owner = owner;
    }

    @Override
    public void copyHolderWaitListsInto( SimpleBitSet waitList )
    {
        owner.copyWaitListTo( waitList );
    }

    @Override
    public int detectDeadlock( int client )
    {
        return owner.isWaitingFor( client ) ? owner.id() : -1;
    }

    @Override
    public String describeWaitList()
    {
        return "ExclusiveLock[" + owner.describeWaitList() + "]";
    }

    @Override
    public void collectOwners( Set<ForsetiClient> owners )
    {
        owners.add( owner );
    }

    @Override
    public LockType type()
    {
        return EXCLUSIVE;
    }

    @Override
    public LongSet transactionIds()
    {
        return LongSets.immutable.of( owner.transactionId() );
    }

    @Override
    public String toString()
    {
        return "ExclusiveLock{" +
               "owner=" + owner +
               '}';
    }
}
