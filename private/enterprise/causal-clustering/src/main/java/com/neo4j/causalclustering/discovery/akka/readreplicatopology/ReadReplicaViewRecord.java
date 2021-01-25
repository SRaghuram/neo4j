/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaViewRecord
{
    private final ReadReplicaInfo readReplicaInfo;
    private final Instant timestamp;
    private final ActorRef topologyClientActorRef;
    private final ServerId serverId;
    private final Map<DatabaseId,DiscoveryDatabaseState> databaseStates;

    ReadReplicaViewRecord( ReadReplicaInfo readReplicaInfo, ActorRef topologyClientActorRef, ServerId serverId, Instant timestamp,
            Map<DatabaseId,DiscoveryDatabaseState> databaseStates )
    {
        this.readReplicaInfo = readReplicaInfo;
        this.timestamp = timestamp;
        this.topologyClientActorRef = topologyClientActorRef;
        this.serverId = serverId;
        this.databaseStates = databaseStates;
    }

    ReadReplicaViewRecord( ReadReplicaRefreshMessage message, Clock clock )
    {
        this( message.readReplicaInfo(), message.topologyClientActorRef(), message.serverId(), Instant.now( clock ), message.databaseStates() );
    }

    public ReadReplicaInfo readReplicaInfo()
    {
        return readReplicaInfo;
    }

    public Instant timestamp()
    {
        return timestamp;
    }

    public ActorRef topologyClientActorRef()
    {
        return topologyClientActorRef;
    }

    public ServerId serverId()
    {
        return serverId;
    }

    Map<DatabaseId,DiscoveryDatabaseState> databaseStates()
    {
        return databaseStates;
    }

    @Override
    public String toString()
    {
        return "ReadReplicaViewRecord{" + "readReplicaInfo=" + readReplicaInfo + ", timestamp=" + timestamp + ", topologyClientActorRef=" +
                topologyClientActorRef + ", serverId=" + serverId + ", databaseState=" + databaseStates + '}';
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
        ReadReplicaViewRecord that = (ReadReplicaViewRecord) o;
        return Objects.equals( readReplicaInfo, that.readReplicaInfo ) && Objects.equals( timestamp, that.timestamp ) &&
                Objects.equals( topologyClientActorRef, that.topologyClientActorRef ) && Objects.equals( serverId, that.serverId ) &&
                Objects.equals( databaseStates, that.databaseStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( readReplicaInfo, timestamp, topologyClientActorRef, serverId, databaseStates );
    }
}
