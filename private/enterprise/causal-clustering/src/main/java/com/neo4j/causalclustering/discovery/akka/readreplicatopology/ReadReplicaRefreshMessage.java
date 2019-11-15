/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import akka.cluster.client.ClusterClient;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.AkkaTopologyClient;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Objects;

import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaRefreshMessage
{
    private final ReadReplicaInfo readReplicaInfo;
    private final MemberId memberId;
    private final ActorRef clusterClient;
    private final ActorRef topologyClient;
    private final Map<DatabaseId,DiscoveryDatabaseState> databaseStates;

    public ReadReplicaRefreshMessage( ReadReplicaInfo readReplicaInfo, MemberId memberId, ActorRef clusterClient, ActorRef topologyClient,
            Map<DatabaseId,DiscoveryDatabaseState> databaseStates )
    {
        this.readReplicaInfo = readReplicaInfo;
        this.memberId = memberId;
        this.clusterClient = clusterClient;
        this.topologyClient = topologyClient;
        this.databaseStates = databaseStates;
    }

    public ReadReplicaInfo readReplicaInfo()
    {
        return readReplicaInfo;
    }

    public MemberId memberId()
    {
        return memberId;
    }

    public Map<DatabaseId,DiscoveryDatabaseState> databaseStates()
    {
        return databaseStates;
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
        return "ReadReplicaRefreshMessage{" + "readReplicaInfo=" + readReplicaInfo + ", memberId=" + memberId + ", clusterClient=" + clusterClient +
                ", topologyClient=" + topologyClient + ", databaseStates=" + databaseStates + '}';
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
        ReadReplicaRefreshMessage that = (ReadReplicaRefreshMessage) o;
        return Objects.equals( readReplicaInfo, that.readReplicaInfo ) && Objects.equals( memberId, that.memberId ) &&
                Objects.equals( clusterClient, that.clusterClient ) && Objects.equals( topologyClient, that.topologyClient ) &&
                Objects.equals( databaseStates, that.databaseStates );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( readReplicaInfo, memberId, clusterClient, topologyClient, databaseStates );
    }
}
