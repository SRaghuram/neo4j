/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClientReceptionist;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.AbstractActorWithTimersAndLogging;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ReadReplicaViewActor extends AbstractActorWithTimersAndLogging
{
    static Props props( ActorRef parent, ClusterClientReceptionist receptionist, Clock clock, Duration refresh )
    {
        return Props.create( ReadReplicaViewActor.class, () -> new ReadReplicaViewActor( parent, receptionist, clock, refresh ) );
    }

    static final String READ_REPLICA_TOPIC = "rr-topic";
    static final int TICKS_BEFORE_REMOVE_READ_REPLICA = 3;
    private static final String TICK_KEY = "Tick key";

    private final ActorRef parent;
    private final ClusterClientReceptionist receptionist;
    private final Clock clock;
    private final Duration refresh;
    private Map<ActorRef,ReadReplicaViewRecord> clusterClientReadReplicas = new HashMap<>();

    private ReadReplicaViewActor( ActorRef parent, ClusterClientReceptionist receptionist, Clock clock, Duration refresh )
    {
        this.parent = parent;
        this.receptionist = receptionist;
        this.clock = clock;
        this.refresh = refresh;
    }

    @Override
    public void preStart()
    {
        receptionist.registerSubscriber( READ_REPLICA_TOPIC, getSelf() );
        getTimers().startPeriodicTimer( TICK_KEY, PruneReplicaViewMessage.getInstance(), refresh );
    }

    @Override
    public void postStop()
    {
        receptionist.unregisterSubscriber( READ_REPLICA_TOPIC, getSelf() );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder
                .create()
                .match( ReadReplicaRefreshMessage.class, this::handleRefreshMessage )
                .match( ReadReplicaRemovalMessage.class, this::handleRemovalMessage )
                .match( PruneReplicaViewMessage.class,   this::handleTick )
                .build();
    }

    private void handleRemovalMessage( ReadReplicaRemovalMessage msg )
    {
        ReadReplicaViewRecord removed = clusterClientReadReplicas.remove( msg.clusterClient() );
        log().debug( "Removed shut down read replica {} -> {}", msg.clusterClient(), removed );
        sendClusterView();
    }

    private void handleRefreshMessage( ReadReplicaRefreshMessage msg )
    {
        log().debug( "Received {}", msg );
        clusterClientReadReplicas.put( msg.clusterClient(), new ReadReplicaViewRecord( msg, clock ) );
        sendClusterView();
    }

    private void handleTick( PruneReplicaViewMessage tick )
    {
        Instant nTicksAgo = Instant.now( clock ).minus( refresh.multipliedBy( TICKS_BEFORE_REMOVE_READ_REPLICA ) );

        List<ActorRef> remove = clusterClientReadReplicas.entrySet()
                .stream()
                .filter( entry -> entry.getValue().timestamp().isBefore( nTicksAgo ) )
                .peek( entry -> log().debug( "Removing {} after inactivity", entry ) )
                .map( Map.Entry::getKey )
                .collect( Collectors.toList() );

        if ( !remove.isEmpty() )
        {
            remove.forEach( clusterClientReadReplicas::remove );
            sendClusterView();
        }

        parent.tell( tick, getSelf() );
    }

    private void sendClusterView()
    {
        parent.tell( new ReadReplicaViewMessage( clusterClientReadReplicas ), getSelf() );
    }

    static class PruneReplicaViewMessage
    {
        private static PruneReplicaViewMessage instance = new PruneReplicaViewMessage();

        private PruneReplicaViewMessage()
        {
        }

        public static PruneReplicaViewMessage getInstance()
        {
            return instance;
        }
    }
}
