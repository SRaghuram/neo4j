/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log;

import java.util.Objects;

public class LogPosition
{
    public long logIndex;
    public long byteOffset;

    public LogPosition( long logIndex, long byteOffset )
    {
        this.logIndex = logIndex;
        this.byteOffset = byteOffset;
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
        LogPosition position = (LogPosition) o;
        return logIndex == position.logIndex && byteOffset == position.byteOffset;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( logIndex, byteOffset );
    }
}
