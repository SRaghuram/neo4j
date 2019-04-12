/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;

import java.time.Duration;
import java.util.Objects;

import org.neo4j.causalclustering.identity.ClusterId;

/**
 * Sent from this Neo4J instance into discovery service during cluster binding
 */
public class ClusterIdSetRequest
{
    private final ClusterId clusterId;
    private final String database;
    private final Duration timeout;
    private final ActorRef replyTo;

    public ClusterIdSetRequest( ClusterId clusterId, String database, Duration timeout )
    {
        this( clusterId, database, timeout, ActorRef.noSender() );
    }

    private ClusterIdSetRequest( ClusterId clusterId, String database, Duration timeout, ActorRef replyTo )
    {
        this.clusterId = clusterId;
        this.database = database;
        this.timeout = timeout;
        this.replyTo = replyTo;
    }

    ClusterIdSetRequest withReplyTo( ActorRef replyTo )
    {
        return new ClusterIdSetRequest( clusterId(), database(), timeout, replyTo );
    }

    ActorRef replyTo()
    {
        return replyTo;
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public String database()
    {
        return database;
    }

    public Duration timeout()
    {
        return timeout;
    }

    @Override
    public String toString()
    {
        return "ClusterIdSetRequest{" + "clusterId=" + clusterId + ", database='" + database + '\'' + '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof ClusterIdSetRequest) )
        {
            return false;
        }
        ClusterIdSetRequest that = (ClusterIdSetRequest) o;
        return Objects.equals( clusterId, that.clusterId ) && Objects.equals( database, that.database );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clusterId, database );
    }
}
