/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorRef;

import java.util.Objects;

public class ReadReplicaRemovalMessage
{
    private final ActorRef topologyClient;

    public ReadReplicaRemovalMessage( ActorRef topologyClient )
    {
        this.topologyClient = topologyClient;
    }

    public ActorRef clusterClient()
    {
        return topologyClient;
    }

    @Override
    public String toString()
    {
        return "ReadReplicaRemovalMessage{" + "topologyClient=" + topologyClient + '}';
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
        return Objects.equals( topologyClient, that.topologyClient );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( topologyClient );
    }
}
