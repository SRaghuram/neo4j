/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.identity.MemberId;

import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;

public class ReadReplicaViewRecord
{
    private final ReadReplicaInfo readReplicaInfo;
    private final Instant timestamp;
    private final ActorRef topologyClientActorRef;
    private final MemberId memberId;
    private final Map<DatabaseId,DatabaseState> databaseStates;

    ReadReplicaViewRecord( ReadReplicaInfo readReplicaInfo, ActorRef topologyClientActorRef, MemberId memberId, Instant timestamp,
            Map<DatabaseId,DatabaseState> databaseStates )
    {
        this.readReplicaInfo = readReplicaInfo;
        this.timestamp = timestamp;
        this.topologyClientActorRef = topologyClientActorRef;
        this.memberId = memberId;
        this.databaseStates = databaseStates;
    }

    ReadReplicaViewRecord( ReadReplicaRefreshMessage message, Clock clock )
    {
        this( message.readReplicaInfo(), message.topologyClientActorRef(), message.memberId(), Instant.now( clock ), message.databaseStates() );
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

    public MemberId memberId()
    {
        return memberId;
    }

    Map<DatabaseId,DatabaseState> databaseStates()
    {
        return databaseStates;
    }

    @Override
    public String toString()
    {
        return "ReadReplicaViewRecord{" + "readReplicaInfo=" + readReplicaInfo + ", timestamp=" + timestamp + ", topologyClientActorRef=" +
                topologyClientActorRef + ", memberId=" + memberId + ", databaseState=" + databaseStates + '}';
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
                Objects.equals( topologyClientActorRef, that.topologyClientActorRef ) && Objects.equals( memberId, that.memberId ) &&
                Objects.equals( databaseStates, that.databaseStates );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( readReplicaInfo, timestamp, topologyClientActorRef, memberId, databaseStates );
    }
}
