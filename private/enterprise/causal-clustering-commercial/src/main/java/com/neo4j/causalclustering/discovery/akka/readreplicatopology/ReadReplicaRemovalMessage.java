/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;

import java.util.Objects;

public class ReadReplicaRemovalMessage
{
    private final ActorRef clusterClient;

    public ReadReplicaRemovalMessage( ActorRef clusterClient )
    {
        this.clusterClient = clusterClient;
    }

    public ActorRef clusterClient()
    {
        return clusterClient;
    }

    @Override
    public String toString()
    {
        return "ReadReplicaRemovalMessage{" + "clusterClient=" + clusterClient + '}';
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
        ReadReplicaRemovalMessage that = (ReadReplicaRemovalMessage) o;
        return Objects.equals( clusterClient, that.clusterClient );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clusterClient );
    }
}
