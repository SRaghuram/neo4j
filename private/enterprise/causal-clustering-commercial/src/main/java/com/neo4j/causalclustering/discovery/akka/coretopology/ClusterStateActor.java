/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.japi.pf.ReceiveBuilder;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Subscribes to events indicating a change in cluster state, maintains a view of current cluster state, and feeds it back to the {@link CoreTopologyActor}
 *
 * This partially replicates functionality in {@link ClusterEvent.CurrentClusterState}. That however is not suitable: it is also updated by cluster events,
 * an actor that subscribes to cluster events may receive those events before CurrentClusterState, so if CurrentClusterState is accessed on a cluster event
 * it may be stale. Furthermore if no further cluster events are received then the updated CurrentClusterState may never be accessed.
 */
public class ClusterStateActor extends AbstractActor
{
    private final Cluster cluster;
    private final ActorRef topologyActor;
    private final Log log;
    private ClusterViewMessage clusterView = ClusterViewMessage.EMPTY;

    static Props props( Cluster cluster, ActorRef topologyActor, LogProvider logProvider )
    {
        return Props.create( ClusterStateActor.class, () -> new ClusterStateActor( cluster, topologyActor, logProvider ) );
    }

    public ClusterStateActor( Cluster cluster, ActorRef topologyActor, LogProvider logProvider )
    {
        this.cluster = cluster;
        this.topologyActor = topologyActor;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void preStart()
    {
        cluster.subscribe( getSelf(), ClusterEvent.initialStateAsSnapshot(), ClusterEvent.ClusterDomainEvent.class, ClusterEvent.UnreachableMember.class );
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
                .match( ClusterEvent.CurrentClusterState.class, event -> {
                    clusterView = new ClusterViewMessage( event );
                    log.debug( "Akka initial cluster state %s", event );
                    sendClusterView();
                } ).match( ClusterEvent.ReachableMember.class, event ->
                {
                    log.debug( "Akka cluster event %s", event );
                    clusterView = clusterView.withoutUnreachable( event.member() );
                    sendClusterView();
                } ).match( ClusterEvent.UnreachableMember.class, event ->
                {
                    log.debug( "Akka cluster event %s", event );
                    clusterView = clusterView.withUnreachable( event.member() );
                    sendClusterView();
                } ).match( ClusterEvent.MemberUp.class, event ->
                {
                    log.debug( "Akka cluster event %s", event );
                    clusterView = clusterView.withMember( event.member() );
                    sendClusterView();
                } ).match( ClusterEvent.MemberWeaklyUp.class, event ->
                {
                    log.debug( "Akka cluster event %s", event );
                    clusterView = clusterView.withMember( event.member() );
                    sendClusterView();
                } ).match( ClusterEvent.MemberRemoved.class, event ->
                {
                    log.debug( "Akka cluster event %s", event );
                    clusterView = clusterView.withoutMember( event.member() );
                    sendClusterView();
                } ).match( ClusterEvent.LeaderChanged.class, event ->
                {
                    log.debug( "Akka cluster event %s", event );
                    clusterView = clusterView.withConverged( event.leader().isDefined() );
                    sendClusterView();
                } ).match( ClusterEvent.ClusterDomainEvent.class, event ->
                {
                    log.debug( "Ignoring Akka cluster event %s", event );
                } ).match( ClusterEvent.MemberEvent.class, event ->
                {
                    log.debug( "Ignoring Akka cluster event %s", event );
                } ).build();
    }

    private void sendClusterView()
    {
        topologyActor.tell( clusterView, getSelf() );
    }
}
