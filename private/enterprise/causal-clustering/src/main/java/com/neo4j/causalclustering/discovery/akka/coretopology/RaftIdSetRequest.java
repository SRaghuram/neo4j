/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.time.Duration;
import java.util.Objects;

/**
 * Sent from this Neo4J instance into discovery service
 */
public class RaftIdSetRequest
{
    private final RaftId raftId;
    private final MemberId publisher;
    private final Duration timeout;
    private final ActorRef replyTo;

    public RaftIdSetRequest( RaftId raftId, MemberId publisher, Duration timeout )
    {
        this( raftId, publisher, timeout, ActorRef.noSender() );
    }

    private RaftIdSetRequest( RaftId raftId, MemberId publisher, Duration timeout, ActorRef replyTo )
    {
        this.raftId = raftId;
        this.publisher = publisher;
        this.timeout = timeout;
        this.replyTo = replyTo;
    }

    RaftIdSetRequest withReplyTo( ActorRef replyTo )
    {
        return new RaftIdSetRequest( raftId, publisher, timeout, replyTo );
    }

    ActorRef replyTo()
    {
        return replyTo;
    }

    public RaftId raftId()
    {
        return raftId;
    }

    public MemberId publisher()
    {
        return publisher;
    }

    public Duration timeout()
    {
        return timeout;
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
        RaftIdSetRequest that = (RaftIdSetRequest) o;
        return Objects.equals( raftId, that.raftId ) && Objects.equals( publisher, that.publisher );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( raftId, publisher );
    }

    @Override
    public String toString()
    {
        return "RaftIdSetRequest{" + "raftId=" + raftId + ", publisher=" + publisher + ", timeout=" + timeout + ", replyTo=" + replyTo + '}';
    }
}
