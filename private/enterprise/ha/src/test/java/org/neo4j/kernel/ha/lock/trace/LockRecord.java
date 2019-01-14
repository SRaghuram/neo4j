/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock.trace;

import java.util.Objects;

import org.neo4j.storageengine.api.lock.ResourceType;

public class LockRecord
{
    private boolean exclusive;
    private ResourceType resourceType;
    private long resourceId;

    public static LockRecord of( boolean exclusive, ResourceType resourceType, long resourceId )
    {
        return new LockRecord( exclusive, resourceType, resourceId );
    }

    private LockRecord( boolean exclusive, ResourceType resourceType, long resourceId )
    {
        this.exclusive = exclusive;
        this.resourceType = resourceType;
        this.resourceId = resourceId;
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
        LockRecord that = (LockRecord) o;
        return exclusive == that.exclusive && resourceId == that.resourceId &&
                Objects.equals( resourceType, that.resourceType );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( exclusive, resourceType, resourceId );
    }
}
