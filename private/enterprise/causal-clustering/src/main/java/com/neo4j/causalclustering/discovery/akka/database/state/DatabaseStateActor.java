/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.database.state;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.LWWMap;
import akka.cluster.ddata.LWWMapKey;
import akka.japi.pf.ReceiveBuilder;
import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.BaseReplicatedDataActor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.DATABASE_STATE;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;

public class DatabaseStateActor extends BaseReplicatedDataActor<LWWMap<DatabaseToMember,DiscoveryDatabaseState>>
{
    public static Props props( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<ReplicatedDatabaseState> discoveryUpdateSink,
            ActorRef rrTopologyActor, ReplicatedDataMonitor monitor, MemberId memberId )
    {
        return Props.create( DatabaseStateActor.class, () -> new DatabaseStateActor( cluster, replicator, discoveryUpdateSink, rrTopologyActor,
                monitor, memberId ) );
    }

    public static final String NAME = "cc-database-status-actor";

    private final SourceQueueWithComplete<ReplicatedDatabaseState> stateUpdateSink;
    private final ActorRef rrTopologyActor;
    private final MemberId memberId;

    private DatabaseStateActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<ReplicatedDatabaseState> stateUpdateSink,
            ActorRef rrTopologyActor, ReplicatedDataMonitor monitor, MemberId memberId )
    {
       super( cluster, replicator, LWWMapKey::create, LWWMap::create, DATABASE_STATE, monitor );
       this.stateUpdateSink = stateUpdateSink;
       this.rrTopologyActor = rrTopologyActor;
       this.memberId = memberId;
    }

    @Override
    protected int dataMetricVisible()
    {
        return data.size();
    }

    @Override
    protected int dataMetricInvisible()
    {
        return data.underlying().keys().vvector().size();
    }

    @Override
    protected void sendInitialDataToReplicator()
    {
        // no-op
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( DiscoveryDatabaseState.class, this::handleDatabaseState );
    }

    private void handleDatabaseState( DiscoveryDatabaseState update )
    {
        if ( update.operatorState() == DROPPED )
        {
            modifyReplicatedData( key, map -> map.remove( cluster, new DatabaseToMember( update.databaseId(), memberId ) ) );
        }
        else
        {
            modifyReplicatedData( key, map -> map.put( cluster, new DatabaseToMember( update.databaseId(), memberId ), update ) );
        }
    }

    @Override
    protected void handleIncomingData( LWWMap<DatabaseToMember,DiscoveryDatabaseState> newData )
    {
        data = newData;
        var statesGroupedByDatabase = data.getEntries().entrySet().stream()
                .map( e -> Map.entry( e.getKey().memberId(), e.getValue() ) )
                .collect( Collectors.groupingBy( e -> e.getValue().databaseId(), entriesToMap() ) );

        var allReplicatedStates = statesGroupedByDatabase.entrySet().stream()
                .map( e -> ReplicatedDatabaseState.ofCores( e.getKey(), e.getValue() ) )
                .collect( Collectors.toList() );

        allReplicatedStates.forEach( stateUpdateSink::offer );
        rrTopologyActor.tell( new AllReplicatedDatabaseStates( allReplicatedStates ), getSelf() );
    }

    private static <K,V> Collector<Map.Entry<K,V>,?,Map<K,V>> entriesToMap()
    {
        return Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue );
    }
}
