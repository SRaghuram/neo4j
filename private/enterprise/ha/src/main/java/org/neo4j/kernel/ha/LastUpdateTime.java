/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

public class LastUpdateTime
{
    private long lastUpdateTime;

    public LastUpdateTime()
    {
        lastUpdateTime = 0;
    }

    public long getLastUpdateTime()
    {
        return lastUpdateTime;
    }

    public void setLastUpdateTime( long lastUpdateTime )
    {
        this.lastUpdateTime = lastUpdateTime;
    }
}
