/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;

import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.identity.MemberId;

public class ReadReplicaViewRecord
{
    private final ReadReplicaInfo readReplicaInfo;
    private final Instant timestamp;
    private final ActorRef topologyClientActorRef;
    private final MemberId memberId;

    public ReadReplicaViewRecord( ReadReplicaInfo readReplicaInfo, ActorRef topologyClientActorRef, MemberId memberId, Instant timestamp )
    {
        this.readReplicaInfo = readReplicaInfo;
        this.timestamp = timestamp;
        this.topologyClientActorRef = topologyClientActorRef;
        this.memberId = memberId;
    }

    ReadReplicaViewRecord( ReadReplicaRefreshMessage message, Clock clock )
    {
        this( message.readReplicaInfo(), message.topologyClientActorRef(), message.memberId(), Instant.now( clock ) );
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

    @Override
    public String toString()
    {
        return "ReadReplicaViewRecord{" + "readReplicaInfo=" + readReplicaInfo + ", timestamp=" + timestamp + ", topologyClientActorRef=" +
                topologyClientActorRef + ", memberId=" + memberId + '}';
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
                Objects.equals( topologyClientActorRef, that.topologyClientActorRef ) && Objects.equals( memberId, that.memberId );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( readReplicaInfo, timestamp, topologyClientActorRef, memberId );
    }
}
