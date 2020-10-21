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
import com.neo4j.causalclustering.discovery.akka.PublishInitialData;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStartedMessage;
import com.neo4j.causalclustering.discovery.akka.common.DatabaseStoppedMessage;
import com.neo4j.causalclustering.discovery.akka.monitoring.ClusterSizeMonitor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.util.VisibleForTesting;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class CoreTopologyActor extends AbstractActorWithTimersAndLogging
{
    public static Props props(
            SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink,
            ActorRef rrTopologyActor,
            ActorRef replicator,
            Cluster cluster,
            TopologyBuilder topologyBuilder,
            Config config,
            ReplicatedDataMonitor replicatedDataMonitor,
            ClusterSizeMonitor clusterSizeMonitor,
            ServerId myself )
    {
        return Props.create( CoreTopologyActor.class,
                () -> new CoreTopologyActor( topologyUpdateSink, bootstrapStateSink, rrTopologyActor, replicator,
                                             cluster, topologyBuilder, config, replicatedDataMonitor, clusterSizeMonitor, myself ) );
    }

    public static final String NAME = "cc-core-topology-actor";

    private final SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink;
    private final SourceQueueWithComplete<BootstrapState> bootstrapStateSink;
    private final TopologyBuilder topologyBuilder;
    private final UniqueAddress myClusterAddress;
    private final Config config;

    private final ActorRef metadataActor;
    private final ActorRef raftIdActor;
    private final ActorRef readReplicaTopologyActor;

    // Topology component data
    private Set<DatabaseId> knownDatabaseIds = emptySet();
    private MetadataMessage memberData;
    private Map<RaftId,RaftMemberId> bootstrappedRafts;
    private ClusterViewMessage clusterView;
    private boolean memberUp;
    private boolean haveObservedSelfInClusterViewOnce;

    private CoreTopologyActor( SourceQueueWithComplete<CoreTopologyMessage> topologyUpdateSink,
            SourceQueueWithComplete<BootstrapState> bootstrapStateSink,
            ActorRef readReplicaTopologyActor,
            ActorRef replicator,
            Cluster cluster,
            TopologyBuilder topologyBuilder,
            Config config,
            ReplicatedDataMonitor replicatedDataMonitor,
            ClusterSizeMonitor clusterSizeMonitor,
            ServerId myself )
    {
        this.topologyUpdateSink = topologyUpdateSink;
        this.bootstrapStateSink = bootstrapStateSink;
        this.readReplicaTopologyActor = readReplicaTopologyActor;
        this.topologyBuilder = topologyBuilder;
        int minCoreHostsAtRuntime = config.get( CausalClusteringSettings.minimum_core_cluster_size_at_runtime );
        this.memberData = MetadataMessage.EMPTY;
        this.bootstrappedRafts = Map.of();
        this.clusterView = ClusterViewMessage.EMPTY;
        this.myClusterAddress = cluster.selfUniqueAddress();
        this.config = config;

        // Children, who will be sending messages to us
        metadataActor = getContext().actorOf( MetadataActor.props( cluster, replicator, getSelf(),
                                                                   config, replicatedDataMonitor, myself ) );
        ActorRef downingActor = getContext().actorOf( ClusterDowningActor.props( cluster, config ) );
        getContext().actorOf( ClusterStateActor.props( cluster, getSelf(), downingActor, metadataActor, config, clusterSizeMonitor ) );
        raftIdActor = getContext().actorOf( RaftIdActor.props( cluster, replicator, getSelf(), replicatedDataMonitor, minCoreHostsAtRuntime ) );

        cluster.registerOnMemberUp( this::onMemberUp );
    }

    @VisibleForTesting
    void onMemberUp()
    {
        this.memberUp = true;
        // trigger self to build topologies. Do it by sending a message because onMemberUp might be called outside of the actor's message-handling loop
        // and so violate our synchronization requirements
        self().tell( new MemberUp(), ActorRef.noSender() );
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
                .match( PublishInitialData.class,        this::handlePublishInitialDataMessage )
                .match( MemberUp.class,                  this::handleMemberUpMessage )
                .build();
    }

    private void handleClusterViewMessage( ClusterViewMessage message )
    {
        clusterView = message;
        if ( !haveObservedSelfInClusterViewOnce && clusterView.availableMembers().anyMatch( myClusterAddress::equals ) )
        {
            haveObservedSelfInClusterViewOnce = true;
        }
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

    private void handleMemberUpMessage( MemberUp message )
    {
        buildTopologies();
    }

    private void handleRaftIdSetRequest( RaftIdSetRequest message )
    {
        raftIdActor.forward( message, getContext() );
    }

    private void handleDatabaseStartedMessage( DatabaseStartedMessage message )
    {
        metadataActor.forward( message, getContext() );
    }

    private void handleDatabaseStoppedMessage( DatabaseStoppedMessage message )
    {
        metadataActor.forward( message, getContext() );
    }

    private void handlePublishInitialDataMessage( PublishInitialData message )
    {
        metadataActor.forward( message, getContext() );
        raftIdActor.forward( message, getContext() );
    }

    private void buildTopologies()
    {
        if ( !isReadyToBuildTopologies() )
        {
            return;
        }

        var receivedDatabaseIds = memberData.getStream()
                .flatMap( info -> info.coreServerInfo().startedDatabaseIds().stream() )
                .collect( toSet() );

        var absentDatabaseIds = knownDatabaseIds.stream()
                .filter( id -> !receivedDatabaseIds.contains( id ) )
                .collect( toSet() );

        // build empty topologies for database IDs cached locally but absent from the set of received database IDs
        absentDatabaseIds.forEach( this::buildTopology );

        // build topologies for the set of received database IDs
        receivedDatabaseIds.forEach( this::buildTopology );

        knownDatabaseIds = receivedDatabaseIds; // override the set of known IDs to no accumulate deleted ones

        var bootstrapped =  new BootstrapState( clusterView, memberData, myClusterAddress, config, bootstrappedRafts );
        bootstrapStateSink.offer( bootstrapped );
    }

    private boolean isReadyToBuildTopologies()
    {
        return memberUp && haveObservedSelfInClusterViewOnce && memberData != null;
    }

    private void buildTopology( DatabaseId databaseId )
    {
        log().debug( "Building new view of core topology from actor {}, cluster state is: {}, metadata is {}",
                myClusterAddress, clusterView, memberData );

        var raftId = RaftId.from( databaseId );
        raftId = bootstrappedRafts.containsKey( raftId ) ? raftId : null;

        DatabaseCoreTopology newCoreTopology = topologyBuilder.buildCoreTopology( databaseId, raftId, clusterView, memberData );
        log().debug( "Returned topology: {}", newCoreTopology );

        Collection<Address> akkaMemberAddresses = clusterView.members()
                .stream()
                .map( Member::address )
                .filter( addr -> !addr.equals( myClusterAddress.address() ) )
                .collect( Collectors.toList() );

        topologyUpdateSink.offer( new CoreTopologyMessage( newCoreTopology, akkaMemberAddresses ) );
        readReplicaTopologyActor.tell( newCoreTopology, getSelf() );
    }

    private static class MemberUp
    {
    }
}
