package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.cluster.client.ClusterClient;
import akka.cluster.client.ClusterClientSettings;
import akka.japi.pf.ReceiveBuilder;
import com.neo4j.causalclustering.discovery.akka.AbstractActorWithTimersAndLogging;

import java.util.function.Supplier;

/**
 * Actor which wraps akka's {{@link ClusterClient}} and forwards any messages to it.
 * It monitors the ClusterClient for termination and recreates it if required.
 */
public class ClusterClientManager extends AbstractActorWithTimersAndLogging
{
    public static final String NAME = "restarting-cluster-client";

    public static Props props( Supplier<ClusterClientSettings> clientSettingsFactory )
    {
        return Props.create( ClusterClientManager.class, () -> new ClusterClientManager( clientSettingsFactory ) );
    }

    private final Supplier<ClusterClientSettings> clientSettingsFactory;
    private ActorRef clusterClient;

    private ClusterClientManager( Supplier<ClusterClientSettings> clientSettingsFactory )
    {
        this.clientSettingsFactory = clientSettingsFactory;
    }

    @Override
    public void preStart()
    {
        this.clusterClient = createClusterClient();
        getContext().watch( clusterClient );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( Terminated.class, this::restartTerminatedClient )
                .matchAny( this::handleOtherMessage )
                .build();
    }

    private void handleOtherMessage( Object msg )
    {
        if ( !getSender().equals( clusterClient ) )
        {
            clusterClient.forward( msg, getContext() );
        }
    }

    private void restartTerminatedClient( Terminated ignored )
    {
        log().warning( "Read replica's discovery client wasn't able to contact any Cores and needed to be restarted. " +
                       "Make sure your `initial_discovery_members` are correct." );

        this.clusterClient = createClusterClient();
        getContext().watch( clusterClient );
    }

    private ActorRef createClusterClient()
    {
        var clientSettings = clientSettingsFactory.get();
        return getContext().actorOf( ClusterClient.props( clientSettings ), "cluster-client" );
    }
}
