/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.common.RaftMemberKnownMessage;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.discovery.akka.directory.LeaderInfoDirectoryMessage;
import com.neo4j.causalclustering.discovery.member.ServerSnapshot;
import com.neo4j.configuration.CausalClusteringSettings;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;

public class ClientTopologyActor extends AbstractActorWithTimers
{
    private static final String REFRESH = "topology refresh";
    private static final int REFRESHES_BEFORE_REMOVE_TOPOLOGY = 3;

    public static Props props( ServerSnapshot serverSnapshot, SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink,
            SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoverySink,
            SourceQueueWithComplete<ReplicatedDatabaseState> stateSink, SourceQueueWithComplete<ReplicatedRaftMapping> raftMappingSink,
            ActorRef clusterClientManager, Config config, LogProvider logProvider, Clock clock, ServerId myself )
    {
        return Props.create( ClientTopologyActor.class,
                () -> new ClientTopologyActor( serverSnapshot, coreTopologySink, rrTopologySink, discoverySink, stateSink, raftMappingSink,
                                               config, logProvider, clock, clusterClientManager, myself ) );
    }

    public static final String NAME = "cc-client-topology-actor";

    private final Duration refreshDuration;
    private final ServerId myself;
    private final ServerSnapshot serverSnapshot;
    private final PruningStateSink<DatabaseCoreTopology> coreTopologySink;
    private final PruningStateSink<DatabaseReadReplicaTopology> readreplicaTopologySink;
    private final PruningStateSink<ReplicatedDatabaseState> coresDbStateSink;
    private final PruningStateSink<ReplicatedDatabaseState> readReplicasDbStateSink;
    private final SourceQueueWithComplete<ReplicatedRaftMapping> raftMappingSink;
    private final SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> discoverySink;
    private final Map<DatabaseId,DiscoveryDatabaseState> localDatabaseStates;
    private final ActorRef clusterClientManager;
    private final Config config;
    private final Log log;

    private final Set<DatabaseId> startedDatabases = new HashSet<>();

    private ClientTopologyActor( ServerSnapshot serverSnapshot, SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink,
            SourceQueueWithComplete<DatabaseReadReplicaTopology> rrTopologySink, SourceQueueWithComplete<Map<DatabaseId,LeaderInfo>> leaderInfoSink,
            SourceQueueWithComplete<ReplicatedDatabaseState> stateSink, SourceQueueWithComplete<ReplicatedRaftMapping> raftMappingSink, Config config,
            LogProvider logProvider, Clock clock, ActorRef clusterClientManager, ServerId myself )
    {
        this.serverSnapshot = serverSnapshot;
        this.refreshDuration = config.get( CausalClusteringSettings.cluster_topology_refresh );
        this.myself = myself;
        var maxTopologyLifetime = refreshDuration.multipliedBy( REFRESHES_BEFORE_REMOVE_TOPOLOGY );
        this.coreTopologySink = PruningStateSink.forCoreTopologies( coreTopologySink, maxTopologyLifetime, clock, logProvider );
        this.readreplicaTopologySink = PruningStateSink.forReadReplicaTopologies( rrTopologySink, maxTopologyLifetime, clock, logProvider );
        this.coresDbStateSink = PruningStateSink.forCoreDatabaseStates( stateSink, maxTopologyLifetime, clock, logProvider );
        this.readReplicasDbStateSink = PruningStateSink.forReadReplicaDatabaseStates( stateSink, maxTopologyLifetime, clock, logProvider );
        this.discoverySink = leaderInfoSink;
        this.raftMappingSink = raftMappingSink;
        this.clusterClientManager = clusterClientManager;
        this.localDatabaseStates = new HashMap<>();
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public Receive createReceive()
    {
        return ReceiveBuilder.create()
                .match( DatabaseCoreTopology.class, coreTopologySink::offer )
                .match( DatabaseReadReplicaTopology.class, readreplicaTopologySink::offer )
                .match( LeaderInfoDirectoryMessage.class, msg -> discoverySink.offer( msg.leaders() ) )
                .match( ReplicatedDatabaseState.class, this::handleRemoteDatabaseStateUpdate )
                .match( TopologiesRefresh.class, ignored -> handleRefresh() )
                .match( DatabaseStartedMessage.class, this::handleDatabaseStartedMessage )
                .match( RaftMemberKnownMessage.class, this::handleRaftMemberKnownMessage )
                .match( DatabaseStoppedMessage.class, this::handleDatabaseStoppedMessage )
                .match( DiscoveryDatabaseState.class, this::handleLocalDatabaseStateUpdate )
                .match( ReplicatedRaftMapping.class, this::handleReplicatedRaftMapping )
                .build();
    }

    @Override
    public void preStart()
    {
        getTimers().startPeriodicTimer( REFRESH, TopologiesRefresh.INSTANCE, refreshDuration );
        var databaseIds = serverSnapshot.discoverableDatabases();
        startedDatabases.addAll( databaseIds );
        sendReadReplicaInfo();
    }

    private void handleDatabaseStartedMessage( DatabaseStartedMessage message )
    {
        if ( startedDatabases.add( message.namedDatabaseId().databaseId() ) )
        {
            sendReadReplicaInfo();
        }
    }

    private void handleRaftMemberKnownMessage( RaftMemberKnownMessage message )
    {
        if ( startedDatabases.add( message.namedDatabaseId().databaseId() ) )
        {
            sendReadReplicaInfo();
        }
    }

    private void handleDatabaseStoppedMessage( DatabaseStoppedMessage message )
    {
        if ( startedDatabases.remove( message.namedDatabaseId().databaseId() ) )
        {
            sendReadReplicaInfo();
        }
    }

    private void handleRemoteDatabaseStateUpdate( ReplicatedDatabaseState update )
    {
        if ( update.containsCoreStates() )
        {
            coresDbStateSink.offer( update );
        }
        else
        {
            readReplicasDbStateSink.offer( update );
        }
    }

    private void handleLocalDatabaseStateUpdate( DiscoveryDatabaseState update )
    {
        if ( update.operatorState() == DROPPED )
        {
            localDatabaseStates.remove( update.databaseId() );
        }
        else
        {
            localDatabaseStates.put( update.databaseId(), update );
        }
    }

    private void handleReplicatedRaftMapping( ReplicatedRaftMapping mapping )
    {
        raftMappingSink.offer( mapping );
    }

    private void handleRefresh()
    {
        coreTopologySink.pruneStaleState();
        readreplicaTopologySink.pruneStaleState();

        sendReadReplicaInfo();
    }

    private void sendReadReplicaInfo()
    {
        var databaseIds = Set.copyOf( startedDatabases );
        var readReplicaInfo = ReadReplicaInfo.from( config, databaseIds );
        var refreshMsg = new ReadReplicaRefreshMessage( readReplicaInfo, myself, clusterClientManager, getSelf(), localDatabaseStates );
        sendToCore( refreshMsg );
    }

    @Override
    public void postStop()
    {
        ReadReplicaRemovalMessage msg = new ReadReplicaRemovalMessage( clusterClientManager );
        log.debug( "Shutting down and sending removal message: %s", msg );
        sendToCore( msg );
    }

    private void sendToCore( Object msg )
    {
        clusterClientManager.tell( new ClusterClient.Publish( ReadReplicaViewActor.READ_REPLICA_TOPIC, msg ), getSelf() );
    }

    private static class TopologiesRefresh
    {
        private static final TopologiesRefresh INSTANCE = new TopologiesRefresh();

        private TopologiesRefresh()
        {
        }
    }
}
