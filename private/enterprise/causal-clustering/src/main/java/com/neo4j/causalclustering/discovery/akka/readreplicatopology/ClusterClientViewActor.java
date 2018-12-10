/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClientUnreachable;
import akka.cluster.client.ClusterClientUp;
import akka.cluster.client.ClusterClients;
import akka.cluster.client.SubscribeClusterClients;
import akka.cluster.client.UnsubscribeClusterClients;
import akka.japi.pf.ReceiveBuilder;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class ClusterClientViewActor extends AbstractActor
{
    static Props props( ActorRef parent, ActorRef receptionist, LogProvider logProvider )
    {
        return Props.create( ClusterClientViewActor.class, () -> new ClusterClientViewActor( parent, receptionist, logProvider ) );
    }

    private final Log log;
    private final ActorRef parent;
    private final ActorRef receptionist;
    private Set<ActorRef> clusterClients = new HashSet<>();

    private ClusterClientViewActor( ActorRef parent, ActorRef receptionist, LogProvider logProvider )
    {
        this.parent = parent;
        this.receptionist = receptionist;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void preStart()
    {
        receptionist.tell( SubscribeClusterClients.getInstance(), getSelf() );
    }

    @Override
    public void postStop()
    {
        receptionist.tell( UnsubscribeClusterClients.getInstance(), getSelf() );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder
                .create()
                .match( ClusterClients.class,           this::handleClusterClients )
                .match( ClusterClientUp.class,          this::handleClusterClientUp )
                .match( ClusterClientUnreachable.class, this::handleClusterClientUnreachable )
                .build();
    }

    private void handleClusterClients( ClusterClients msg )
    {
        log.debug( "All cluster clients: %s", msg );
        clusterClients.addAll( msg.getClusterClients() );
        sendToParent();
    }

    private void handleClusterClientUp( ClusterClientUp msg )
    {
        log.debug( "Cluster client up: %s", msg );
        clusterClients.add( msg.clusterClient() );
        sendToParent();
    }

    private void handleClusterClientUnreachable( ClusterClientUnreachable msg )
    {
        log.debug( "Cluster client down: %s", msg );
        clusterClients.remove( msg.clusterClient() );
        sendToParent();
    }

    private void sendToParent()
    {
        parent.tell( new ClusterClientViewMessage( clusterClients ), getSelf() );
    }
}
