/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.AbstractLoggingActor;
import akka.actor.Address;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.InternalClusterAction;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageSpy;
import com.neo4j.configuration.CausalClusteringInternalSettings;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;

public class ClusterDowningActor extends AbstractLoggingActor
{
    public static Props props( Cluster cluster, Config config )
    {
        var downUnreachableOnJoin = config.get( CausalClusteringInternalSettings.middleware_akka_down_unreachable_on_new_joiner );
        return Props.create( ClusterDowningActor.class, () -> new ClusterDowningActor( cluster, downUnreachableOnJoin ) );
    }

    private final Cluster cluster;
    private final boolean downUnreachableOnJoin;

    public ClusterDowningActor( Cluster cluster, Boolean downUnreachableOnJoin )
    {
        this.cluster = cluster;
        this.downUnreachableOnJoin = downUnreachableOnJoin;
    }

    @Override
    public void preStart()
    {
        if ( downUnreachableOnJoin )
        {
            subscribeToInitJoinRequests();
        }
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder
                .create()
                .match( JoinMessageSpy.InitJoinRequestObserved.class, this::handleInitJoin )
                .match( ClusterViewMessage.class, this::handle )
                .build();
    }

    private void handleInitJoin( JoinMessageSpy.InitJoinRequestObserved wrapper )
    {
        InternalClusterAction.InitJoin originalMessage = wrapper.getOriginalMessage();
        log().debug( "Received '{}' message from {}: {}", originalMessage.getClass(), wrapper.getOriginalSender(), originalMessage );

        ClusterEvent.CurrentClusterState clusterState = cluster.state();
        Set<Member> unreachable = clusterState.getUnreachable();

        Set<Member> upAndReachableMembers = new HashSet<>();
        for ( Member m : clusterState.getMembers() )
        {
            if ( !unreachable.contains( m ) && m.status() == MemberStatus.up() )
            {
                upAndReachableMembers.add( m );
            }
        }

        if ( upAndReachableMembers.size() <= unreachable.size() && unreachable.size() > 0 )
        {
            // TODO: is it possible to just down enough members to let the new one join? (always down the sender's address though)
            List<Address> addresses = clusterState.getUnreachable().stream()
                                                  .map( Member::address )
                                                  .collect( Collectors.toList() );

            log().info( "Downing unreachable cores ({}) that were preventing a new core ({}) from joining discovery",
                        addresses.stream().map( Address::toString ).collect( Collectors.joining( ", " ) ), wrapper.getOriginalSender() );

            addresses.forEach( cluster::down );
        }
    }

    private void handle( ClusterViewMessage clusterView )
    {
        if ( clusterView.mostAreReachable() && clusterView.unreachable().size() > 0 )
        {
            log().info( "Downing members: {}", clusterView.unreachable() );

            clusterView
                    .unreachable()
                    .stream()
                    .map( Member::address )
                    .forEach( cluster::down );
        }
        else if ( !clusterView.mostAreReachable() )
        {
            log().info( "In minority side of network partition? {}", clusterView );
        }
    }

    private void subscribeToInitJoinRequests()
    {
        boolean subscribed = cluster.system().eventStream().subscribe( getSelf(), JoinMessageSpy.InitJoinRequestObserved.class );
        if ( !subscribed )
        {
            log().warning( "Unable to subscribe to system event stream" );
        }
    }
}
