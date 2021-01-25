/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.akka.AbstractActorWithTimersAndLogging;
import com.neo4j.causalclustering.discovery.akka.coretopology.RestartNeededListeningActor;
import com.neo4j.configuration.CausalClusteringInternalSettings;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.hostname;

public class ClusterJoiningActor extends AbstractActorWithTimersAndLogging
{
    static final String AKKA_SCHEME = "akka";
    private final int minMembersAtFormation;
    private final ActorRef restartNeededListeningActor;
    private final boolean allowAnyCoreToBootstrapAkkaCluster;

    public static Props props( Cluster cluster, ActorRef restartNeededListeningActor, RemoteMembersResolver resolver, Config config )
    {
        return Props.create( ClusterJoiningActor.class, () -> new ClusterJoiningActor( cluster, restartNeededListeningActor, resolver, config ) );
    }

    public static final String NAME = "joiner";

    private static final String TIMER = "join timer";

    private final Cluster cluster;
    private final RemoteMembersResolver remoteMembersResolver;
    private final Duration retry;

    private ClusterJoiningActor( Cluster cluster, ActorRef restartNeededListeningActor, RemoteMembersResolver remoteMembersResolver, Config config )
    {
        this.cluster = cluster;
        this.restartNeededListeningActor = restartNeededListeningActor;
        this.remoteMembersResolver = remoteMembersResolver;
        this.retry = config.get( CausalClusteringInternalSettings.cluster_binding_retry_timeout );
        this.minMembersAtFormation = config.get( CausalClusteringInternalSettings.middleware_akka_min_number_of_members_at_formation );
        this.allowAnyCoreToBootstrapAkkaCluster = config.get( CausalClusteringInternalSettings.middleware_akka_allow_any_core_to_bootstrap );
    }

    @Override
    public void preStart()
    {
        cluster.registerOnMemberUp( () -> {
            logJoinedDiscovery( cluster.state().getMembers() );
            getTimers().cancel( TIMER );
            getContext().stop( getSelf() );
        } );
    }

    private void logJoinedDiscovery( Iterable<Member> members1 )
    {
        List<String> memberAddresses = new LinkedList<>();

        for ( Member m : members1 )
        {
            memberAddresses.add( m.address().host().get() + ":" + m.address().port().get() );
        }
        log().info( "Joined discovery with members: {}", String.join( ",", memberAddresses ) );
        log().debug( "Join successful, exiting" );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( JoinMessage.class, this::join )
                .build();
    }

    /**
     * Messages are sent to this method to trigger an attempt to join or form an akka cluster (which we do here by calling Cluster#joinSeedNodes).
     * The akka documentation for cluster joining/formation is here: https://doc.akka.io/docs/akka/current/typed/cluster.html#tuning-joins.
     *
     * If allowAnyCoreToBootstrapAkkaCluster is set then we will iterate (slowly) over all the addresses as first seed node - this could be useful if
     * customers want to form clusters after a disaster such as loss of a data center or significant network partition. We have to do this rotation because
     * Akka treats the first address in the list of seed nodes as special and it is the only member that is permitted to "join itself" in order to bootstrap
     * an Akka cluster.
     *
     * N.B. if the remoteMemberResolver has resolveOnEveryJoinAttempt() -> true then setting allowAnyCoreToBootstrapAkkaCluster will have NO EFFECT. In these
     * cases the method being used to resolve the membership (e.g. DNS or K8s) is relied on to not return members who have failed irrecoverably - so rotating
     * the list of seed nodes is not required.
     *
     * @param message the message to handle
     */
    private void join( JoinMessage message )
    {
        log().debug( "Processing: {}", message );
        if ( sendActorSystemRestartIfNecessary() )
        {
            // no need to set another timer here or anything here because restarting the actor system will sent a new inital join message
            return;
        }
        if ( message.isResolveRequired() )
        {
            ArrayList<Address> seedNodes = resolve();
            seedNodes.addAll( message.all().stream().filter( a -> !seedNodes.contains( a ) ).collect( Collectors.toList() ) );

            log().info( "Joining seed nodes: {}", seedNodes );
            cluster.joinSeedNodes( seedNodes );
            startTimer( remoteMembersResolver.resolveOnEveryJoinAttempt() ? message : message.withResolvedAddresses( seedNodes ) );
        }
        else if ( !message.hasAddress() )
        {
            throw new IllegalStateException( "JoinMessage must either contain addresses to join or have resolveRequired flag set" );
        }
        else
        {
            List<Address> addresses = message.all();
            log().info( "Joining seed nodes: {}", addresses );
            cluster.joinSeedNodes( addresses );
            startTimer( allowAnyCoreToBootstrapAkkaCluster ? message.withRotatedAddresses() : message );
        }
    }

    private boolean sendActorSystemRestartIfNecessary()
    {
        ClusterEvent.CurrentClusterState currentClusterState = cluster.state();
        Member myself = cluster.selfMember();

        if ( currentClusterState != null
             && currentClusterState.members().size() > 0
             && currentClusterState.members().size() < minMembersAtFormation
             && currentClusterState.members().contains( myself )
             && currentClusterState.getUnreachable().size() == 0
        )
        {
            log().debug( "Detected attempt to form discovery cluster with lone seed node" );
            restartNeededListeningActor.tell( new RestartNeededListeningActor.SingletonSeedClusterDetected(), this.self() );
            return true;
        }
        return false;
    }

    // The following call potentially blocks, but as it only happens when not connected it shouldn't cause a problem
    private ArrayList<Address> resolve()
    {
        return remoteMembersResolver.resolve( this::toAkkaAddress, ArrayList::new );
    }

    private void startTimer( JoinMessage message )
    {
        getTimers().startSingleTimer( TIMER, message, retry );
    }

    private Address toAkkaAddress( SocketAddress resolvedAddress )
    {
        return new Address( AKKA_SCHEME, getContext().getSystem().name(), hostname( resolvedAddress ), resolvedAddress.getPort() );
    }
}
