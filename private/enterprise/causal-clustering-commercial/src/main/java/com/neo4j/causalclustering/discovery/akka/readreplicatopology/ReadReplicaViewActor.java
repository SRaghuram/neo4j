/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClientReceptionist;
import akka.japi.pf.ReceiveBuilder;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class ReadReplicaViewActor extends AbstractActorWithTimers
{
    static Props props( ActorRef parent, ClusterClientReceptionist receptionist, Clock clock, Duration refresh, LogProvider logProvider )
    {
        return Props.create( ReadReplicaViewActor.class, () -> new ReadReplicaViewActor( parent, receptionist, clock, refresh, logProvider ) );
    }

    static final String READ_REPLICA_TOPIC = "rr-topic";
    static final int TICKS_BEFORE_REMOVE_READ_REPLICA = 3;
    private static final String TICK_KEY = "Tick key";

    private final ActorRef parent;
    private final ClusterClientReceptionist receptionist;
    private final Clock clock;
    private final Duration refresh;
    private final Log log;
    private Map<ActorRef,ReadReplicaViewRecord> clusterClientReadReplicas = new HashMap<>();

    private ReadReplicaViewActor( ActorRef parent, ClusterClientReceptionist receptionist, Clock clock, Duration refresh, LogProvider logProvider )
    {
        this.parent = parent;
        this.receptionist = receptionist;
        this.clock = clock;
        this.refresh = refresh;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void preStart()
    {
        receptionist.registerSubscriber( READ_REPLICA_TOPIC, getSelf() );
        getTimers().startPeriodicTimer( TICK_KEY, Tick.getInstance(), refresh );
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
                .match( Tick.class,                      this::handleTick )
                .build();
    }

    private void handleRemovalMessage( ReadReplicaRemovalMessage msg )
    {
        ReadReplicaViewRecord removed = clusterClientReadReplicas.remove( msg.clusterClient() );
        log.debug( "Removed shut down read replica %s -> %s", msg.clusterClient(), removed );
        sendClusterView();
    }

    private void handleRefreshMessage( ReadReplicaRefreshMessage msg )
    {
        log.debug( "Received %s", msg );
        clusterClientReadReplicas.put( msg.clusterClient(), new ReadReplicaViewRecord( msg, clock ) );
        sendClusterView();
    }

    private void handleTick( Tick tick )
    {
        Instant nTicksAgo = Instant.now( clock ).minus( refresh.multipliedBy( TICKS_BEFORE_REMOVE_READ_REPLICA ) );

        List<ActorRef> remove = clusterClientReadReplicas.entrySet()
                .stream()
                .filter( entry -> entry.getValue().timestamp().isBefore( nTicksAgo ) )
                .peek( entry -> log.debug( "Removing %s after inactivity", entry ) )
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

    static class Tick
    {
        private static Tick instance = new Tick();

        private Tick()
        {
        }

        public static Tick getInstance()
        {
            return instance;
        }
    }
}
