/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.Address;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.akka.AbstractActorWithTimersAndLogging;
import com.neo4j.configuration.CausalClusteringInternalSettings;

import java.time.Duration;
import java.util.ArrayList;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;

import static com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.hostname;

public class ClusterJoiningActor extends AbstractActorWithTimersAndLogging
{
    static final String AKKA_SCHEME = "akka";

    public static Props props( Cluster cluster, RemoteMembersResolver resolver, Config config )
    {
        return Props.create( ClusterJoiningActor.class, () -> new ClusterJoiningActor( cluster, resolver, config ) );
    }

    public static final String NAME = "joiner";

    private static final String TIMER = "join timer";

    private final Cluster cluster;
    private final RemoteMembersResolver remoteMembersResolver;
    private final Duration retry;

    private ClusterJoiningActor( Cluster cluster, RemoteMembersResolver remoteMembersResolver, Config config )
    {
        this.cluster = cluster;
        this.remoteMembersResolver = remoteMembersResolver;
        this.retry = config.get( CausalClusteringInternalSettings.cluster_binding_retry_timeout );
    }

    @Override
    public void preStart()
    {
        cluster.registerOnMemberUp( () ->
        {
            log().debug( "Join successful, exiting" );
            getContext().stop( getSelf() );
        } );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( JoinMessage.class, this::join )
                .build();
    }

    private void join( JoinMessage message )
    {
        log().debug( "Processing: {}", message );
        if ( !message.isReJoin() )
        {
            ArrayList<Address> seedNodes = resolve();
            log().info( "Joining seed nodes: {}", seedNodes );
            cluster.joinSeedNodes( seedNodes );
            startTimer( message );
        }
        else if ( !message.hasAddress() )
        {
            ArrayList<Address> seedNodes = resolve();
            getSelf().tell( JoinMessage.initial( message.isReJoin(), seedNodes ), getSelf() );
        }
        else if ( message.head().equals( cluster.selfAddress() ) )
        {
            log().info( "Not joining to self. Retrying next." );
            getSelf().tell( message.tailMsg(), getSelf() );
        }
        else
        {
            Address address = message.head();
            log().info( "Attempting to join: {}", address );
            cluster.join( address );
            startTimer( message.tailMsg() );
        }
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
