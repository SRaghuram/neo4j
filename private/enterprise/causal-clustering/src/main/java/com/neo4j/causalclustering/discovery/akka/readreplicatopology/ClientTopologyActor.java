/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.client.ClusterClient;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryMember;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClientTopologyActor extends AbstractActorWithTimers
{
    private static final String REFRESH = "topology refresh";

    public static Props props( DiscoveryMember myself, SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink,
            SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoverySink,
            ActorRef clusterClient, Config config, LogProvider logProvider )
    {
        return Props.create( ClientTopologyActor.class,
                () -> new ClientTopologyActor( myself, coreTopologySink, rrTopologySink, discoverySink, clusterClient, config, logProvider ) );
    }

    public static final String NAME = "cc-client-topology-actor";

    private final Duration refresh;
    private final DiscoveryMember myself;
    private final SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink;
    private final SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink;
    private final SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoverySink;
    private final ActorRef clusterClient;
    private final Config config;
    private final Log log;

    private final Set<DatabaseId> startedDatabases = new HashSet<>();

    private ClientTopologyActor( DiscoveryMember myself, SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink,
            SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoverySink,
            ActorRef clusterClient, Config config, LogProvider logProvider )
    {
        this.myself = myself;
        this.coreTopologySink = coreTopologySink;
        this.rrTopologySink = rrTopologySink;
        this.discoverySink = discoverySink;
        this.clusterClient = clusterClient;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
        this.refresh = config.get( CausalClusteringSettings.cluster_topology_refresh );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( DatabaseCoreTopology.class, coreTopologySink::offer )
                .match( DatabaseReadReplicaTopology.class, rrTopologySink::offer )
                .match( LeaderInfoDirectoryMessage.class, msg -> discoverySink.offer( msg.leaders() ) )
                .match( Refresh.class, ignored -> sendReadReplicaInfo() )
                .match( DatabaseStartedMessage.class, this::handleDatabaseStartedMessage )
                .match( DatabaseStoppedMessage.class, this::handleDatabaseStoppedMessage )
                .build();
    }

    @Override
    public void preStart()
    {
        getTimers().startPeriodicTimer( REFRESH, Refresh.instance, refresh );
        startedDatabases.addAll( myself.startedDatabases() );
        sendReadReplicaInfo();
    }

    private void handleDatabaseStartedMessage( DatabaseStartedMessage message )
    {
        if ( startedDatabases.add( message.databaseId() ) )
        {
            sendReadReplicaInfo();
        }
    }

    private void handleDatabaseStoppedMessage( DatabaseStoppedMessage message )
    {
        if ( startedDatabases.remove( message.databaseId() ) )
        {
            sendReadReplicaInfo();
        }
    }

    private void sendReadReplicaInfo()
    {
        var databaseIds = Set.copyOf( startedDatabases );
        var readReplicaInfo = ReadReplicaInfo.from( config, databaseIds );
        var refreshMsg = new ReadReplicaRefreshMessage( readReplicaInfo, myself.id(), clusterClient, getSelf() );
        sendToCore( refreshMsg );
    }

    @Override
    public void postStop()
    {
        ReadReplicaRemovalMessage msg = new ReadReplicaRemovalMessage( clusterClient );
        log.debug( "Shutting down and sending removal message: %s", msg );
        sendToCore( msg );
    }

    private void sendToCore( Object msg )
    {
        clusterClient.tell( new ClusterClient.Publish( ReadReplicaViewActor.READ_REPLICA_TOPIC, msg ), getSelf() );
    }

    private static final class Refresh
    {
        private static final Refresh instance = new Refresh();
        private Refresh()
        {
        }

        public static Refresh getInstance()
        {
            return instance;
        }
    }
}
