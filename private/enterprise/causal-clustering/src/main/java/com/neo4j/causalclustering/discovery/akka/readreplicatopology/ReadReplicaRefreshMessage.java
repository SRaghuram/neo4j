/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.AkkaTopologyClient;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;

import java.util.Map;
import java.util.Objects;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaRefreshMessage
{
    private final ReadReplicaInfo readReplicaInfo;
    private final ServerId serverId;
    private final ActorRef clusterClientManager;
    private final ActorRef topologyClient;
    private final Map<DatabaseId,DiscoveryDatabaseState> databaseStates;

    public ReadReplicaRefreshMessage( ReadReplicaInfo readReplicaInfo, ServerId serverId, ActorRef clusterClientManager, ActorRef topologyClient,
            Map<DatabaseId,DiscoveryDatabaseState> databaseStates )
    {
        this.readReplicaInfo = readReplicaInfo;
        this.serverId = serverId;
        this.clusterClientManager = clusterClientManager;
        this.topologyClient = topologyClient;
        this.databaseStates = databaseStates;
    }

    public ReadReplicaInfo readReplicaInfo()
    {
        return readReplicaInfo;
    }

    public ServerId serverId()
    {
        return serverId;
    }

    public Map<DatabaseId,DiscoveryDatabaseState> databaseStates()
    {
        return databaseStates;
    }

    /**
     * @return {@link ActorRef} for {@link ClusterClientManager}
     */
    public ActorRef clusterClientManager()
    {
        return clusterClientManager;
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
        return "ReadReplicaRefreshMessage{" + "readReplicaInfo=" + readReplicaInfo + ", serverId=" + serverId + ", clusterClient=" + clusterClientManager +
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
        return Objects.equals( readReplicaInfo, that.readReplicaInfo ) && Objects.equals( serverId, that.serverId ) &&
               Objects.equals( clusterClientManager, that.clusterClientManager ) && Objects.equals( topologyClient, that.topologyClient ) &&
               Objects.equals( databaseStates, that.databaseStates );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( readReplicaInfo, serverId, clusterClientManager, topologyClient, databaseStates );
    }
}
