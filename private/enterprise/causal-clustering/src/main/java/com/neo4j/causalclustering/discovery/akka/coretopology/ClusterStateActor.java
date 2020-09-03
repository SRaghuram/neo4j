/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.AbstractActorWithTimersAndLogging;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;

import java.time.Duration;

import org.neo4j.configuration.Config;

import static com.neo4j.configuration.CausalClusteringInternalSettings.akka_failure_detector_acceptable_heartbeat_pause;
import static com.neo4j.configuration.CausalClusteringInternalSettings.akka_failure_detector_heartbeat_interval;

/**
 * Subscribes to events indicating a change in cluster state, maintains a view of current cluster state, and feeds it back to the {@link CoreTopologyActor}
 *
 * This partially replicates functionality in {@link ClusterEvent.CurrentClusterState}. That however is not suitable: it is also updated by cluster events,
 * an actor that subscribes to cluster events may receive those events before CurrentClusterState, so if CurrentClusterState is accessed on a cluster event
 * it may be stale. Furthermore if no further cluster events are received then the updated CurrentClusterState may never be accessed.
 */
public class ClusterStateActor extends AbstractActorWithTimersAndLogging
{
    static Props props( Cluster cluster, ActorRef topologyActor, ActorRef downingActor, ActorRef metadataActor,
            Config config, ClusterSizeMonitor monitor )
    {
        return Props.create( ClusterStateActor.class, () ->
                new ClusterStateActor( cluster, topologyActor, downingActor, metadataActor, config, monitor ) );
    }

    private final Cluster cluster;
    private final ActorRef topologyActor;
    private final ActorRef downingActor;
    private final ActorRef metadataActor;
    private final Duration clusterStabilityWait;
    private final ClusterSizeMonitor monitor;

    private ClusterViewMessage clusterView = ClusterViewMessage.EMPTY;

    private static final String DOWNING_TIMER_KEY = "DOWNING_TIMER_KEY key";
    private static final String MONITOR_TICK_KEY = "MONITOR_TICK_KEY tick";

    public ClusterStateActor( Cluster cluster, ActorRef topologyActor, ActorRef downingActor, ActorRef metadataActor,
            Config config, ClusterSizeMonitor monitor )
    {
        this.cluster = cluster;
        this.topologyActor = topologyActor;
        this.downingActor = downingActor;
        this.metadataActor = metadataActor;
        this.monitor = monitor;

        clusterStabilityWait = config.get( akka_failure_detector_heartbeat_interval )
                .plus( config.get( akka_failure_detector_acceptable_heartbeat_pause ) );
    }

    @Override
    public void preStart()
    {
        cluster.subscribe( getSelf(), ClusterEvent.initialStateAsSnapshot(), ClusterEvent.ClusterDomainEvent.class, ClusterEvent.UnreachableMember.class );
        getTimers().startPeriodicTimer( MONITOR_TICK_KEY, ClusterMonitorRefresh.INSTANCE, Duration.ofMinutes( 1 ) );
    }

    @Override
    public void postStop()
    {
        cluster.unsubscribe( getSelf() );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( ClusterEvent.CurrentClusterState.class, this::handleCurrentClusterState )
                .match( ClusterEvent.ReachableMember.class,     this::handleReachableMember )
                .match( ClusterEvent.UnreachableMember.class,   this::handleUnreachableMember )
                .match( ClusterEvent.MemberUp.class,            this::handleMemberUp )
                .match( ClusterEvent.MemberWeaklyUp.class,      this::handleMemberWeaklyUp )
                .match( ClusterEvent.MemberRemoved.class,       this::handleMemberRemoved )
                .match( ClusterEvent.LeaderChanged.class,       this::handleLeaderChanged )
                .match( ClusterEvent.ClusterDomainEvent.class,  this::handleOtherClusterEvent )
                .match( StabilityMessage.class,                 this::notifyDowningActor )
                .match( ClusterMonitorRefresh.class,            ignored -> updateMonitor() )
                .build();
    }

    private void handleCurrentClusterState( ClusterEvent.CurrentClusterState event )
    {
        clusterView = new ClusterViewMessage( event );
        log().debug( "Akka initial cluster state {}", event );
        sendClusterView();
    }

    private void handleReachableMember( ClusterEvent.ReachableMember event )
    {
        log().debug( "Akka cluster event {}", event );
        clusterView = clusterView.withoutUnreachable( event.member() );
        sendClusterView();
    }

    private void handleUnreachableMember( ClusterEvent.UnreachableMember event )
    {
        log().debug( "Akka cluster event {}", event );
        clusterView = clusterView.withUnreachable( event.member() );
        sendClusterView();
    }

    private void handleMemberUp( ClusterEvent.MemberUp event )
    {
        log().debug( "Akka cluster event {}", event );
        clusterView = clusterView.withMember( event.member() );
        sendClusterView();
    }

    private void handleMemberWeaklyUp( ClusterEvent.MemberWeaklyUp event )
    {
        log().debug( "Akka cluster event {}", event );
        clusterView = clusterView.withMember( event.member() );
        sendClusterView();
    }

    private void handleMemberRemoved( ClusterEvent.MemberRemoved event )
    {
        log().debug( "Akka cluster event {}", event );
        Member member = event.member();
        clusterView = clusterView.withoutMember( member );
        sendClusterView();
        var cleanupMessage = new CleanupMessage( member.uniqueAddress() );
        metadataActor.tell( cleanupMessage, getSelf() );
    }

    private void handleLeaderChanged( ClusterEvent.LeaderChanged event )
    {
        log().debug( "Akka cluster event {}", event );
        clusterView = clusterView.withConverged( event.leader().isDefined() );
        sendClusterView();
    }

    private void handleOtherClusterEvent( ClusterEvent.ClusterDomainEvent event )
    {
        log().debug( "Ignoring Akka cluster event {}", event );
        resetDowningTimer();
    }

    private void notifyDowningActor( StabilityMessage ignored )
    {
        log().debug( "Cluster is stable at {}", clusterView );
        downingActor.tell( clusterView, getSelf() );
    }

    private void sendClusterView()
    {
        topologyActor.tell( clusterView, getSelf() );
        updateMonitor();
        resetDowningTimer();
    }

    private void resetDowningTimer()
    {
        // will cancel previous timer
        timers().startSingleTimer( DOWNING_TIMER_KEY, StabilityMessage.INSTANCE, clusterStabilityWait );
    }

    private void updateMonitor()
    {
        monitor.setMembers( clusterView.members().size() );
        monitor.setUnreachable( clusterView.unreachable().size() );
        monitor.setConverged( clusterView.converged() );
    }

    private static class StabilityMessage
    {
        static final StabilityMessage INSTANCE = new StabilityMessage();

        private StabilityMessage()
        {
        }
    }

    static class ClusterMonitorRefresh
    {
        static final ClusterMonitorRefresh INSTANCE = new ClusterMonitorRefresh();

        private ClusterMonitorRefresh()
        {
        }
    }
}
