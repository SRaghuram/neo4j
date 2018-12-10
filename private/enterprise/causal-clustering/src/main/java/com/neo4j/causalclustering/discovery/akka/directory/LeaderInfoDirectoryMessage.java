/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.directory;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Sent from discovery service to this Neo4J instance
 */
public class LeaderInfoDirectoryMessage
{
    public static LeaderInfoDirectoryMessage EMPTY = new LeaderInfoDirectoryMessage( Collections.emptyMap() );

    private final Map<String,LeaderInfo> leaders;

    public LeaderInfoDirectoryMessage( Map<String,LeaderInfo> leaders )
    {
        this.leaders = Collections.unmodifiableMap( leaders );
    }

    public Map<String,LeaderInfo> leaders()
    {
        return leaders;
    }

    @Override
    public String toString()
    {
        return "LeaderInfoDirectoryMessage{" + "leaders=" + leaders + '}';
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
        LeaderInfoDirectoryMessage that = (LeaderInfoDirectoryMessage) o;
        return Objects.equals( leaders, that.leaders );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( leaders );
    }
}
