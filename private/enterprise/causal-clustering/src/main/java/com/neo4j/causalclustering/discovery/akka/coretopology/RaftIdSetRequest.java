/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Duration;
import java.util.Objects;

/**
 * Sent from this Neo4J instance into discovery service
 */
public class RaftIdSetRequest
{
    private final RaftGroupId raftGroupId;
    private final RaftMemberId publisher;
    private final Duration timeout;
    private final ActorRef replyTo;

    public RaftIdSetRequest( RaftGroupId raftGroupId, RaftMemberId publisher, Duration timeout )
    {
        this( raftGroupId, publisher, timeout, ActorRef.noSender() );
    }

    private RaftIdSetRequest( RaftGroupId raftGroupId, RaftMemberId publisher, Duration timeout, ActorRef replyTo )
    {
        this.raftGroupId = raftGroupId;
        this.publisher = publisher;
        this.timeout = timeout;
        this.replyTo = replyTo;
    }

    RaftIdSetRequest withReplyTo( ActorRef replyTo )
    {
        return new RaftIdSetRequest( raftGroupId, publisher, timeout, replyTo );
    }

    ActorRef replyTo()
    {
        return replyTo;
    }

    public RaftGroupId raftGroupId()
    {
        return raftGroupId;
    }

    public RaftMemberId publisher()
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
        return Objects.equals( raftGroupId, that.raftGroupId ) && Objects.equals( publisher, that.publisher );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( raftGroupId, publisher );
    }

    @Override
    public String toString()
    {
        return "RaftIdSetRequest{" + "raftGroupId=" + raftGroupId + ", publisher=" + publisher + ", timeout=" + timeout + ", replyTo=" + replyTo + '}';
    }
}
