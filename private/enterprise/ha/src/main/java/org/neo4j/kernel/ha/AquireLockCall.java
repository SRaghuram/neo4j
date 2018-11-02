/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.jboss.netty.buffer.ChannelBuffer;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TargetCaller;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.ResourceType;

abstract class AcquireLockCall implements TargetCaller<Master, LockResult>
{
    @Override
    public Response<LockResult> call( Master master, RequestContext context,
                                      ChannelBuffer input, ChannelBuffer target )
    {
        ResourceType type = ResourceTypes.fromId( input.readInt() );
        long[] ids = new long[input.readInt()];
        for ( int i = 0; i < ids.length; i++ )
        {
            ids[i] = input.readLong();
        }
        return lock( master, context, type, ids );
    }

    protected abstract Response<LockResult> lock( Master master, RequestContext context, ResourceType type,
                                                  long... ids );
}
