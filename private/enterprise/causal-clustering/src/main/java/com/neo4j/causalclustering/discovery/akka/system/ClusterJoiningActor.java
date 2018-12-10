/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.AbstractActorWithTimers;
import akka.actor.Address;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;

import java.time.Duration;
import java.util.ArrayList;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.discovery.akka.system.TypesafeConfigService.hostname;

public class ClusterJoiningActor extends AbstractActorWithTimers
{
    static final String AKKA_SCHEME = "akka";

    public static Props props( Cluster cluster, RemoteMembersResolver resolver, Config config, LogProvider logProvider )
    {
        return Props.create( ClusterJoiningActor.class, () -> new ClusterJoiningActor( cluster, resolver, config, logProvider ) );
    }

    public static final String NAME = "joiner";

    private static final String TIMER = "join timer";

    private final Cluster cluster;
    private final RemoteMembersResolver remoteMembersResolver;
    private final Log log;
    private final Duration retry;

    private ClusterJoiningActor( Cluster cluster, RemoteMembersResolver remoteMembersResolver, Config config, LogProvider logProvider )
    {
        this.cluster = cluster;
        this.remoteMembersResolver = remoteMembersResolver;
        this.log = logProvider.getLog( getClass() );
        this.retry = config.get( CausalClusteringSettings.cluster_binding_retry_timeout );
    }

    @Override
    public void preStart()
    {
        cluster.registerOnMemberUp( () ->
        {
            log.debug( "Join successful, exiting" );
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
        log.debug( "Processing: %s", message );
        if ( !message.isReJoin() )
        {
            ArrayList<Address> seedNodes = resolve();
            log.info( "Joining seed nodes: %s", seedNodes );
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
            log.info( "Not joining to self. Retrying next." );
            getSelf().tell( message.tailMsg(), getSelf() );
        }
        else
        {
            Address address = message.head();
            log.info( "Attempting to join: %s", address );
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

    private Address toAkkaAddress( AdvertisedSocketAddress resolvedAddress )
    {
        return new Address( AKKA_SCHEME, getContext().getSystem().name(), hostname( resolvedAddress ), resolvedAddress.getPort() );
    }
}
