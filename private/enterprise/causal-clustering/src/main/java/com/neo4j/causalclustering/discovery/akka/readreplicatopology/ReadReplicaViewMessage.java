/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.CollectorsUtil;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;

import static java.util.stream.Collectors.toSet;

class ReadReplicaViewMessage
{
    private final Map<ActorRef,ReadReplicaViewRecord> clusterClientReadReplicas;

    static ReadReplicaViewMessage EMPTY = new ReadReplicaViewMessage( Collections.emptyMap() );

    ReadReplicaViewMessage( Map<ActorRef,ReadReplicaViewRecord> clusterClientReadReplicas )
    {
        this.clusterClientReadReplicas = Map.copyOf( clusterClientReadReplicas );
    }

    Stream<ActorRef> topologyClient( ActorRef clusterClient )
    {
        return Stream.ofNullable( clusterClientReadReplicas.get( clusterClient ) )
                .map( ReadReplicaViewRecord::topologyClientActorRef );
    }

    DatabaseReadReplicaTopology toReadReplicaTopology( DatabaseId databaseId )
    {
        Map<MemberId,ReadReplicaInfo> knownReadReplicas = clusterClientReadReplicas
                .values()
                .stream()
                .filter( info -> info.readReplicaInfo().getDatabaseIds().contains( databaseId ) )
                .map( info -> Pair.of( info.memberId(), info.readReplicaInfo() ) )
                .collect( CollectorsUtil.pairsToMap() );

        return new DatabaseReadReplicaTopology( databaseId, knownReadReplicas );
    }

    Set<DatabaseId> databaseIds()
    {
        return clusterClientReadReplicas.values().stream()
                .map( ReadReplicaViewRecord::readReplicaInfo )
                .flatMap( info -> info.getDatabaseIds().stream() )
                .collect( toSet() );
    }

    @Override
    public String toString()
    {
        return "ReadReplicaViewMessage{" + "clusterClientReadReplicas=" + clusterClientReadReplicas + '}';
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
        return Objects.equals( clusterClientReadReplicas, that.clusterClientReadReplicas );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( clusterClientReadReplicas );
    }
}
