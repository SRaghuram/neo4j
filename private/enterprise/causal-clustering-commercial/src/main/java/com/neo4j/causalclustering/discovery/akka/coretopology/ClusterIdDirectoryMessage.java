/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.cluster.ddata.LWWMap;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.neo4j.causalclustering.identity.ClusterId;

/**
 * Sent from discovery service to this Neo4J instance
 */
public class ClusterIdDirectoryMessage
{
    public static final ClusterIdDirectoryMessage EMPTY = new ClusterIdDirectoryMessage( Collections.emptyMap() );
    private final Map<String,ClusterId> data;

    public ClusterIdDirectoryMessage( LWWMap<String,ClusterId> data )
    {
        this.data = data.getEntries();
    }

    public ClusterIdDirectoryMessage( Map<String,ClusterId> data )
    {
        this.data = Collections.unmodifiableMap( data );
    }

    @Nullable
    public ClusterId get( String database )
    {
        return data.get( database );
    }

    @Override
    public String toString()
    {
        return "ClusterIdDirectoryMessage{" + "data=" + data + '}';
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
        ClusterIdDirectoryMessage that = (ClusterIdDirectoryMessage) o;
        return Objects.equals( data, that.data );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( data );
    }
}
