/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.EventStream;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.ThisActorSystemQuarantinedEvent;
import com.neo4j.causalclustering.discovery.akka.AbstractActorWithTimersAndLogging;
import com.neo4j.causalclustering.discovery.akka.AkkaActorSystemRestartStrategy;

import java.time.Duration;

public class RestartNeededListeningActor extends AbstractActorWithTimersAndLogging
{
    public static final String NAME = "cc-core-restart-needed-listener";
    public static final String TIMER_KEY = "cc-core-restart-needed-listener-timer";

    public static Props props( Runnable restart, EventStream eventStream, Cluster cluster,
                               AkkaActorSystemRestartStrategy actorSystemRestartStrategy )
    {
        return Props.create( RestartNeededListeningActor.class,
                             () -> new RestartNeededListeningActor( restart, eventStream, cluster, actorSystemRestartStrategy ) );
    }

    private RestartNeededListeningActor( Runnable restart, EventStream eventStream, Cluster cluster,
                                         AkkaActorSystemRestartStrategy actorSystemRestartStrategy )
    {
        this.restart = restart;
        this.eventStream = eventStream;
        this.cluster = cluster;
        this.actorSystemRestartStrategy = actorSystemRestartStrategy;
    }

    private final Runnable restart;
    private final EventStream eventStream;
    private final Cluster cluster;
    private final AkkaActorSystemRestartStrategy actorSystemRestartStrategy;

    @Override
    public void preStart()
    {
        eventStream.subscribe( getSelf(), ThisActorSystemQuarantinedEvent.class );
        cluster.subscribe( getSelf(), ClusterEvent.ClusterShuttingDown$.class );
        cluster.registerOnMemberUp(
                () -> timers().startPeriodicTimer( TIMER_KEY, new Tick(), actorSystemRestartStrategy.checkFrequency() )
        );
    }

    @Override
    public void postStop()
    {
        timers().cancelAll();
        unsubscribe();
    }

    private void unsubscribe()
    {
        eventStream.unsubscribe( getSelf(), ThisActorSystemQuarantinedEvent.class );
        eventStream.unsubscribe( getSelf(), SingletonSeedClusterDetected.class );
        cluster.unsubscribe( getSelf(), ClusterEvent.ClusterShuttingDown$.class );
        timers().cancelAll();
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                             .match( ThisActorSystemQuarantinedEvent.class,   this::doRestart )
                             .match( ClusterEvent.ClusterShuttingDown$.class, this::doRestart )
                             .match( SingletonSeedClusterDetected.class,      this::doRestart )
                             .match( Tick.class,                              this::considerRestart )
                             .match( ClusterEvent.CurrentClusterState.class,  ignore -> {} )
                             .build();
    }

    private void considerRestart( Object event )
    {
        if ( actorSystemRestartStrategy.restartRequired( cluster ) )
        {
            doRestart( actorSystemRestartStrategy.getReason() );
        }
    }

    private void doRestart( Object event )
    {
        log().info( "Restart triggered by {}", event );
        unsubscribe();
        restart.run();
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

    public static class SingletonSeedClusterDetected
    {
    }

    /*
     * No-op class for timer messages
     */
    public static class Tick
    {
    }
}
