/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;

public class DatabaseLeaderInfoMessage
{
    public static DatabaseLeaderInfoMessage EMPTY = new DatabaseLeaderInfoMessage( Collections.emptyMap() );

    private final Map<String,LeaderInfo> leaders;

    public DatabaseLeaderInfoMessage( Map<String,LeaderInfo> leaders )
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
        return "DatabaseLeaderInfoMessage{" + "leaders=" + leaders + '}';
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
        DatabaseLeaderInfoMessage that = (DatabaseLeaderInfoMessage) o;
        return Objects.equals( leaders, that.leaders );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( leaders );
    }
}
