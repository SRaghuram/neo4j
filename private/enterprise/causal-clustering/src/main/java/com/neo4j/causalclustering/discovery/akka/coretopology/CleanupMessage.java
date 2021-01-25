/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.UniqueAddress;

import java.util.Objects;

public class CleanupMessage
{
    private final UniqueAddress uniqueAddress;

    public CleanupMessage( UniqueAddress uniqueAddress )
    {
        this.uniqueAddress = uniqueAddress;
    }

    public UniqueAddress uniqueAddress()
    {
        return uniqueAddress;
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
        CleanupMessage that = (CleanupMessage) o;
        return Objects.equals( uniqueAddress, that.uniqueAddress );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( uniqueAddress );
    }
}
