/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;

import java.util.Objects;

public class ReadReplicaRemovalMessage
{
    private final ActorRef clusterClientManager;

    public ReadReplicaRemovalMessage( ActorRef clusterClientManager )
    {
        this.clusterClientManager = clusterClientManager;
    }

    public ActorRef clusterClientManager()
    {
        return clusterClientManager;
    }

    @Override
    public String toString()
    {
        return "ReadReplicaRemovalMessage{" + "clusterClientManager=" + clusterClientManager + '}';
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
        return Objects.equals( clusterClientManager, that.clusterClientManager );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clusterClientManager );
    }
}
