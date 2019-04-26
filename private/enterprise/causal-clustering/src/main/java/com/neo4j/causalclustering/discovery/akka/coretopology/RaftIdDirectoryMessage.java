/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.ddata.LWWMap;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

import org.neo4j.kernel.database.DatabaseId;

/**
 * Sent from discovery service to this Neo4J instance
 */
public class RaftIdDirectoryMessage
{
    public static final RaftIdDirectoryMessage EMPTY = new RaftIdDirectoryMessage( Collections.emptyMap() );
    private final Map<DatabaseId,RaftId> data;

    public RaftIdDirectoryMessage( LWWMap<DatabaseId,RaftId> data )
    {
        this.data = data.getEntries();
    }

    public RaftIdDirectoryMessage( Map<DatabaseId,RaftId> data )
    {
        this.data = Collections.unmodifiableMap( data );
    }

    @Nullable
    public RaftId get( DatabaseId databaseId )
    {
        return data.get( databaseId );
    }

    @Override
    public String toString()
    {
        return "RaftIdDirectoryMessage{" + "data=" + data + '}';
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
        RaftIdDirectoryMessage that = (RaftIdDirectoryMessage) o;
        return Objects.equals( data, that.data );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( data );
    }
}
