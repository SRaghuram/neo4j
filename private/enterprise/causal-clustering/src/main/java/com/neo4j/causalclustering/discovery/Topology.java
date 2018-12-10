/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

public interface Topology<T extends DiscoveryServerInfo>
{
    Map<MemberId, T> members();

    default TopologyDifference difference( Topology<T> other )
    {
        Set<MemberId> members = members().keySet();
        Set<MemberId> otherMembers = other.members().keySet();

        Set<Difference> added = otherMembers.stream().filter( m -> !members.contains( m ) )
                .map( memberId -> Difference.asDifference( other, memberId ) ).collect( toSet() );

        Set<Difference> removed = members.stream().filter( m -> !otherMembers.contains( m ) )
                .map( memberId -> Difference.asDifference( this, memberId ) ).collect( toSet() );

        return new TopologyDifference( added, removed );
    }

    default Optional<T> find( MemberId memberId )
    {
        return Optional.ofNullable( members().get( memberId ) );
    }

    default Map<MemberId, T> filterHostsByDb( Map<MemberId, T> s, String dbName )
    {
        return s.entrySet().stream().filter(e -> e.getValue().getDatabaseName().equals( dbName ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    Topology<T> filterTopologyByDb( String dbName );
}
