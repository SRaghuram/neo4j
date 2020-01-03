/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.EventStream;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.ThisActorSystemQuarantinedEvent;

public class RestartNeededListeningActor extends AbstractLoggingActor
{
    public static final String NAME = "cc-core-restart-needed-listener";

    public static Props props( Runnable restart, EventStream eventStream, Cluster cluster )
    {
        return Props.create( RestartNeededListeningActor.class, () -> new RestartNeededListeningActor( restart, eventStream, cluster ) );
    }

    private RestartNeededListeningActor( Runnable restart, EventStream eventStream, Cluster cluster )
    {
        this.restart = restart;
        this.eventStream = eventStream;
        this.cluster = cluster;
    }

    private final Runnable restart;
    private final EventStream eventStream;
    private final Cluster cluster;

    @Override
    public void preStart()
    {
        eventStream.subscribe( getSelf(), ThisActorSystemQuarantinedEvent.class );
        cluster.subscribe( getSelf(), ClusterEvent.ClusterShuttingDown$.class );
    }

    @Override
    public void postStop()
    {
        unsubscribe();
    }

    private void unsubscribe()
    {
        eventStream.unsubscribe( getSelf(), ThisActorSystemQuarantinedEvent.class );
        cluster.unsubscribe( getSelf(), ClusterEvent.ClusterShuttingDown$.class );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( ThisActorSystemQuarantinedEvent.class,   this::doRestart )
                .match( ClusterEvent.ClusterShuttingDown$.class, this::doRestart )
                .match( ClusterEvent.CurrentClusterState.class,  ignore -> {} )
                .build();
    }

    private void doRestart( Object event )
    {
        log().info( "Restart triggered by {}", event );
        restart.run();
        unsubscribe();
        getContext().become( createShuttingDownReceive() );
    }

    private Receive createShuttingDownReceive()
    {
        return ReceiveBuilder.create()
                .match( ThisActorSystemQuarantinedEvent.class,   this::ignore )
                .match( ClusterEvent.ClusterShuttingDown$.class, this::ignore )
                .match( ClusterEvent.CurrentClusterState.class,  ignore -> {} )
                .build();
    }

    private void ignore( Object event )
    {
        log().debug( "Ignoring as restart has been triggered: {}", event );
    }
}
