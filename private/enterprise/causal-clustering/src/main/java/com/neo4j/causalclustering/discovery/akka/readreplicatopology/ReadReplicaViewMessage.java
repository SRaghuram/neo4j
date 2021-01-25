/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorPath;
import akka.actor.ActorRef;
import akka.cluster.client.ClusterClient;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.stream.Collectors.toSet;

class ReadReplicaViewMessage
{
    private final Map<ActorPath,ReadReplicaViewRecord> clientToReadReplicaRecords;

    static final ReadReplicaViewMessage EMPTY = new ReadReplicaViewMessage( Collections.emptyMap() );

    ReadReplicaViewMessage( Map<ActorPath,ReadReplicaViewRecord> readReplicaClients )
    {
        this.clientToReadReplicaRecords = Map.copyOf( readReplicaClients );
    }

    /**
     * Given a list of {{@link ClusterClient}}s, find those which correspond to known Read Replicas
     * and return references for their {{@link ClientTopologyActor}}s.
     *
     * Note: ReadReplicaViewRecords are indexed by the actor path of their {{@link ClusterClientManager}}
     * as their cluster client may be replaced at runtime. We must therefore extract the path of each
     * client's parent manager before performing any filtering.
     */
    Stream<ActorRef> topologyActorsForKnownClients( Collection<ActorRef> clusterClients )
    {
        return clusterClients.stream()
                .map( client -> client.path().parent() )
                .flatMap( client -> Stream.ofNullable( clientToReadReplicaRecords.get( client ) ) )
                .map( ReadReplicaViewRecord::topologyClientActorRef );
    }

    DatabaseReadReplicaTopology toReadReplicaTopology( DatabaseId databaseId )
    {
        Map<ServerId,ReadReplicaInfo> knownReadReplicas = clientToReadReplicaRecords
                .values()
                .stream()
                .filter( info -> info.readReplicaInfo().startedDatabaseIds().contains( databaseId ) )
                .map( info -> Pair.of( info.serverId(), info.readReplicaInfo() ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        return new DatabaseReadReplicaTopology( databaseId, knownReadReplicas );
    }

    Map<DatabaseId,ReplicatedDatabaseState> allReadReplicaDatabaseStates()
    {
        var allMemberStatesPerDbMultiMap = clientToReadReplicaRecords.values().stream()
                .flatMap( this::getAllStatesFromMember )
                .collect( Collectors.groupingBy( p -> p.other().databaseId(), Collectors.toMap( Pair::first, Pair::other ) ) );

        return allMemberStatesPerDbMultiMap.entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, e -> ReplicatedDatabaseState.ofReadReplicas( e.getKey(), e.getValue() ) ) );
    }

    private Stream<Pair<ServerId,DiscoveryDatabaseState>> getAllStatesFromMember( ReadReplicaViewRecord record )
    {
         return record.databaseStates().values().stream().map( state -> Pair.of( record.serverId(), state ) );
    }

    Set<DatabaseId> databaseIds()
    {
        return clientToReadReplicaRecords.values().stream()
                                         .map( ReadReplicaViewRecord::readReplicaInfo )
                                         .flatMap( info -> info.startedDatabaseIds().stream() )
                                         .collect( toSet() );
    }

    @Override
    public String toString()
    {
        return "ReadReplicaViewMessage{" + "clusterClientReadReplicas=" + clientToReadReplicaRecords + '}';
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
        ReadReplicaViewMessage that = (ReadReplicaViewMessage) o;
        return Objects.equals( clientToReadReplicaRecords, that.clientToReadReplicaRecords );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( clientToReadReplicaRecords );
    }
}
