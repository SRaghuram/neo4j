/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class ClusterClientViewMessage
{
    private final Set<ActorRef> clusterClients;

    public ClusterClientViewMessage( Set<ActorRef> clusterClients )
    {
        this.clusterClients = Collections.unmodifiableSet( new HashSet<>( clusterClients ) );
    }

    public Set<ActorRef> clusterClients()
    {
        return clusterClients;
    }

    @Override
    public String toString()
    {
        return "ClusterClientViewMessage{" + "clusterClients=" + clusterClients + '}';
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
        ClusterClientViewMessage that = (ClusterClientViewMessage) o;
        return Objects.equals( clusterClients, that.clusterClients );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( clusterClients );
    }
}
