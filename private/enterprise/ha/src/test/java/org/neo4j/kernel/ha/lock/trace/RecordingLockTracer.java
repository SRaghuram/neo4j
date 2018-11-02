/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock.trace;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.storageengine.api.lock.LockTracer;
import org.neo4j.storageengine.api.lock.LockWaitEvent;
import org.neo4j.storageengine.api.lock.ResourceType;

public class RecordingLockTracer implements LockTracer
{
    private List<LockRecord> requestedLocks = new CopyOnWriteArrayList<>();

    @Override
    public LockWaitEvent waitForLock( boolean exclusive, ResourceType resourceType, long... resourceIds )
    {
        for ( long resourceId : resourceIds )
        {
            requestedLocks.add( LockRecord.of( exclusive, resourceType, resourceId ) );
        }
        return null;
    }

    public List<LockRecord> getRequestedLocks()
    {
        return requestedLocks;
    }
}
