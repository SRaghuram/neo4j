/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.Member;
import akka.cluster.UniqueAddress;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.akka.AbstractActorWithTimersAndLogging;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.member.DiscoveryMember;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.configuration.CausalClusteringInternalSettings;
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class CoreTopologyActor extends AbstractActorWithTimersAndLogging
{
    public static Props props(
            DiscoveryMember myself,
            SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink,
            ActorRef rrTopologyActor,
            ActorRef replicator,
            Cluster cluster,
            TopologyBuilder topologyBuilder,
            Config config,
            ReplicatedDataMonitor replicatedDataMonitor,
            ClusterSizeMonitor clusterSizeMonitor )
    {
        return Props.create( CoreTopologyActor.class,
                () -> new CoreTopologyActor( myself, topologyUpdateSink, bootstrapStateSink, rrTopologyActor, replicator,
                        cluster, topologyBuilder, config, replicatedDataMonitor, clusterSizeMonitor ) );
    }

    public static final String NAME = "cc-core-topology-actor";

    private final SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink;
    private final SourceQueueWithComplete<BootstrapState> bootstrapStateSink;
    private final TopologyBuilder topologyBuilder;
    private final int minCoreHostsAtRuntime;

    private final UniqueAddress myClusterAddress;

    private final Config config;

    private Set<DatabaseId> knownDatabaseIds = emptySet();

    private final ActorRef metadataActor;
    private final ActorRef raftIdActor;
    private final ActorRef readReplicaTopologyActor;

    // Topology component data
    private MetadataMessage memberData;
    private Set<RaftId> bootstrappedRafts;
    private ClusterViewMessage clusterView;

    private CoreTopologyActor( DiscoveryMember myself,
            SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink,
            ActorRef readReplicaTopologyActor,
            ActorRef replicator,
            Cluster cluster,
            TopologyBuilder topologyBuilder,
            Config config,
            ReplicatedDataMonitor replicatedDataMonitor,
            ClusterSizeMonitor clusterSizeMonitor )
    {
        this.topologyUpdateSink = topologyUpdateSink;
        this.bootstrapStateSink = bootstrapStateSink;
        this.readReplicaTopologyActor = readReplicaTopologyActor;
        this.topologyBuilder = topologyBuilder;
        this.minCoreHostsAtRuntime = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_runtime );
        this.memberData = MetadataMessage.EMPTY;
        this.bootstrappedRafts = emptySet();
        this.clusterView = ClusterViewMessage.EMPTY;
        this.myClusterAddress = cluster.selfUniqueAddress();
        this.config = config;

        // Children, who will be sending messages to us
        metadataActor = getContext().actorOf( MetadataActor.props( myself, cluster, replicator, getSelf(), config, replicatedDataMonitor ) );
        ActorRef downingActor = getContext().actorOf( ClusterDowningActor.props( cluster, config ) );
        getContext().actorOf( ClusterStateActor.props( cluster, getSelf(), downingActor, metadataActor, config, clusterSizeMonitor ) );
        raftIdActor = getContext().actorOf( RaftIdActor.props( cluster, replicator, getSelf(), replicatedDataMonitor, minCoreHostsAtRuntime ) );
    }

    @Override
    public Receive createReceive()
    {
        return receiveBuilder()
                .match( ClusterViewMessage.class,        this::handleClusterViewMessage)
                .match( MetadataMessage.class,           this::handleMetadataMessage )
                .match( BootstrappedRaftsMessage.class,  this::handleBootstrappedRaftsMessage )
                .match( RaftIdSetRequest.class,          this::handleRaftIdSetRequest )
                .match( DatabaseStartedMessage.class,    this::handleDatabaseStartedMessage )
                .match( DatabaseStoppedMessage.class,    this::handleDatabaseStoppedMessage )
                .build();
    }

    private void handleClusterViewMessage( ClusterViewMessage message )
    {
        clusterView = message;
        buildTopologies();
    }

    private void handleMetadataMessage( MetadataMessage message )
    {
        memberData = message;
        buildTopologies();
    }

    private void handleBootstrappedRaftsMessage( BootstrappedRaftsMessage message )
    {
        bootstrappedRafts = message.bootstrappedRafts();
        buildTopologies();
    }

    private void handleRaftIdSetRequest( RaftIdSetRequest message )
    {
        raftIdActor.forward( message, getContext() );
    }

    private void handleDatabaseStartedMessage( DatabaseStartedMessage message )
    {
        metadataActor.forward( message, context() );
    }

    private void handleDatabaseStoppedMessage( DatabaseStoppedMessage message )
    {
        metadataActor.forward( message, context() );
    }

    private void buildTopologies()
    {
        var receivedDatabaseIds = memberData.getStream()
                .flatMap( info -> info.coreServerInfo().startedDatabaseIds().stream() )
                .collect( toSet() );

        var absentDatabaseIds = knownDatabaseIds.stream()
                .filter( id -> !receivedDatabaseIds.contains( id ) )
                .collect( toSet() );

        knownDatabaseIds = receivedDatabaseIds; // override the set of known IDs to no accumulate deleted ones

        // build empty topologies for database IDs cached locally but absent from the set of received database IDs
        absentDatabaseIds.forEach( this::buildTopology );

        // build topologies for the set of received database IDs
        receivedDatabaseIds.forEach( this::buildTopology );
    }

    private void buildTopology( DatabaseId databaseId )
    {
        log().debug( "Building new view of core topology from actor {}, cluster state is: {}, metadata is {}",
                myClusterAddress, clusterView, memberData );

        var raftId = RaftId.from( databaseId );
        raftId = bootstrappedRafts.contains( raftId ) ? raftId : null;

        DatabaseCoreTopology newCoreTopology = topologyBuilder.buildCoreTopology( databaseId, raftId, clusterView, memberData );
        log().debug( "Returned topology: {}", newCoreTopology );

        Collection<Address> akkaMemberAddresses = clusterView.members()
                .stream()
                .map( Member::address )
                .filter( addr -> !addr.equals( myClusterAddress.address() ) )
                .collect( Collectors.toList() );

        topologyUpdateSink.offer( new CoreTopologyMessage( newCoreTopology, akkaMemberAddresses ) );
        readReplicaTopologyActor.tell( newCoreTopology, getSelf() );
        bootstrapStateSink.offer( new BootstrapState( clusterView, memberData, myClusterAddress, config ) );
    }
}
