/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import akka.cluster.client.ClusterClient;
import com.neo4j.causalclustering.discovery.akka.AkkaTopologyClient;

import java.util.Objects;

import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.identity.MemberId;

public class ReadReplicaInfoMessage
{
    private final ReadReplicaInfo readReplicaInfo;
    private final MemberId memberId;
    private final ActorRef clusterClient;
    private final ActorRef topologyClient;

    public ReadReplicaInfoMessage( ReadReplicaInfo readReplicaInfo, MemberId memberId, ActorRef clusterClient,
            ActorRef topologyClient )
    {
        this.readReplicaInfo = readReplicaInfo;
        this.memberId = memberId;
        this.clusterClient = clusterClient;
        this.topologyClient = topologyClient;
    }

    public ReadReplicaInfo readReplicaInfo()
    {
        return readReplicaInfo;
    }

    public MemberId memberId()
    {
        return memberId;
    }

    /**
     * @return {@link ActorRef} for {@link ClusterClient}
     */
    public ActorRef clusterClient()
    {
        return clusterClient;
    }

    /**
     * @return {@link ActorRef} for {@link AkkaTopologyClient}
     */
    public ActorRef topologyClientActorRef()
    {
        return topologyClient;
    }

    @Override
    public String toString()
    {
        return "ReadReplicaInfoMessage{" + "readReplicaInfo=" + readReplicaInfo + ", memberId=" + memberId + ", clusterClient=" + clusterClient + "," +
                " topologyClient=" + topologyClient + '}';
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
        ReadReplicaInfoMessage that = (ReadReplicaInfoMessage) o;
        return Objects.equals( readReplicaInfo, that.readReplicaInfo ) && Objects.equals( memberId, that.memberId ) &&
                Objects.equals( clusterClient, that.clusterClient ) &&
                Objects.equals( topologyClient, that.topologyClient );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( readReplicaInfo, memberId, clusterClient, topologyClient );
    }
}
