/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.causalclustering.discovery.akka.coretopology.RaftMemberMappingActor;
import com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataMonitor;
import com.neo4j.causalclustering.discovery.member.ServerSnapshot;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.causalclustering.discovery.akka.monitoring.ReplicatedDataIdentifier.DATABASE_STATE;
import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;

public class DatabaseStateActor extends BaseReplicatedDataActor<LWWMap<DatabaseServer,DiscoveryDatabaseState>>
{
    public static Props props( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<ReplicatedDatabaseState> discoveryUpdateSink,
            ActorRef rrTopologyActor, ReplicatedDataMonitor monitor, ServerId myself )
    {
        return Props.create(
                DatabaseStateActor.class, () -> new DatabaseStateActor( cluster, replicator, discoveryUpdateSink, rrTopologyActor, monitor, myself ) );
    }

    public static final String NAME = "cc-database-status-actor";

    private final SourceQueueWithComplete<ReplicatedDatabaseState> stateUpdateSink;
    private final ActorRef rrTopologyActor;
    private final ServerId myself;

    private DatabaseStateActor( Cluster cluster, ActorRef replicator, SourceQueueWithComplete<ReplicatedDatabaseState> stateUpdateSink,
            ActorRef rrTopologyActor, ReplicatedDataMonitor monitor,
            ServerId myself )
    {
        super( cluster, replicator, LWWMapKey::create, LWWMap::create, DATABASE_STATE, monitor );
        this.stateUpdateSink = stateUpdateSink;
        this.rrTopologyActor = rrTopologyActor;
        this.myself = myself;
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
    protected void sendInitialDataToReplicator( ServerSnapshot serverSnapshot )
    {
        var localStatesMap = serverSnapshot.databaseStates().entrySet().stream()
                                           .reduce( LWWMap.create(), this::addState, LWWMap::merge );

        if ( !localStatesMap.isEmpty() )
        {
            modifyReplicatedData( key, map -> map.merge( localStatesMap ) );
        }
    }

    private LWWMap<DatabaseServer,DiscoveryDatabaseState> addState( LWWMap<DatabaseServer,DiscoveryDatabaseState> acc,
            Map.Entry<DatabaseId,DatabaseState> entry )
    {
        var databaseId = entry.getKey();
        var dbState = entry.getValue();
        var discoveryState = new DiscoveryDatabaseState( databaseId, dbState.operatorState(), dbState.failure().orElse( null ) );
        return acc.put( cluster, new DatabaseServer( databaseId, myself ), discoveryState );
    }

    @Override
    protected void handleCustomEvents( ReceiveBuilder builder )
    {
        builder.match( DatabaseStateActor.CleanupMessage.class, this::removeDataFromReplicator )
               .match( DiscoveryDatabaseState.class,            this::handleDatabaseState );
    }

    private void handleDatabaseState( DiscoveryDatabaseState update )
    {
        if ( update.operatorState() == DROPPED )
        {
            modifyReplicatedData( key, map -> map.remove( cluster, new DatabaseServer( update.databaseId(), myself ) ) );
        }
        else
        {
            modifyReplicatedData( key, map -> map.put( cluster, new DatabaseServer( update.databaseId(), myself ), update ) );
        }
    }

    private void removeDataFromReplicator( DatabaseStateActor.CleanupMessage message )
    {
        data.getEntries().keySet().stream()
                .filter( ds -> ds.serverId().equals( message.serverId ) )
                .peek( ds -> log().info( "remove state {}", ds ) )
                .forEach( ds -> modifyReplicatedData( key, map -> map.remove( cluster, ds ) ));
    }

    @Override
    protected void handleIncomingData( LWWMap<DatabaseServer,DiscoveryDatabaseState> newData )
    {
        data = newData;
        var statesGroupedByDatabase = data.getEntries().entrySet().stream()
                .map( e -> Map.entry( e.getKey().serverId(), e.getValue() ) )
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

    public static class CleanupMessage
    {
        private final ServerId serverId;

        public CleanupMessage( ServerId serverId )
        {
            this.serverId = serverId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            DatabaseStateActor.CleanupMessage that = (DatabaseStateActor.CleanupMessage) o;
            return Objects.equals( serverId, that.serverId );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( serverId );
        }
    }
}
