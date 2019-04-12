/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;

import java.util.Objects;

import org.neo4j.causalclustering.identity.ClusterId;

/**
 * Sent from this Neo4J instance into discovery service
 */
public class ClusterIdSettingMessage
{
    private final ClusterId clusterId;
    private final String database;
    private final ActorRef replyTo;

    public ClusterIdSettingMessage( ClusterId clusterId, String database )
    {
        this.clusterId = clusterId;
        this.database = database;
        this.replyTo = ActorRef.noSender();
    }

    private ClusterIdSettingMessage( ClusterId clusterId, String database, ActorRef replyTo )
    {
        this.clusterId = clusterId;
        this.database = database;
        this.replyTo = replyTo;
    }

    ClusterIdSettingMessage withReplyTo( ActorRef replyTo )
    {
        return new ClusterIdSettingMessage( clusterId, database, replyTo );
    }

    public ClusterId clusterId()
    {
        return clusterId;
    }

    public String database()
    {
        return database;
    }

    @Override
    public String toString()
    {
        return "ClusterIdSettingMessage{" + "clusterId=" + clusterId + ", database='" + database + '}';
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( !(o instanceof ClusterIdSettingMessage) )
        {
            return false;
        }
        ClusterIdSettingMessage that = (ClusterIdSettingMessage) o;
        return Objects.equals( clusterId, that.clusterId ) && Objects.equals( database, that.database );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clusterId, database );
    }

    ActorRef replyTo()
    {
        return replyTo;
    }
}
