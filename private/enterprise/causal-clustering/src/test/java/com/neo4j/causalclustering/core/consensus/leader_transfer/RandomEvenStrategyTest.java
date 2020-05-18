/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;
import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferTarget.NO_TARGET;

public class RandomEvenStrategyTest
{
    @Test
    void onlySelectMembersWithTwoFewerLeaderships()
    {
        // given
        var member1 = new MemberId( UUID.randomUUID() );
        var member2 = new MemberId( UUID.randomUUID() );
        var member3 = new MemberId( UUID.randomUUID() );
        var member4 = new MemberId( UUID.randomUUID() );

        var memberLeaderMap = Map.of(
                member1, databaseIds( 0 ).limit( 4 ).collect( Collectors.toList() ),
                member2, databaseIds( 4 ).limit( 3 ).collect( Collectors.toList() ),
                member3, databaseIds( 7 ).limit( 5 ).collect( Collectors.toList() ),
                member4, databaseIds( 12 ).limit( 9 ).collect( Collectors.toList() ) );

        var dbToLeaderMap = memberLeaderMap.entrySet()
                                           .stream()
                                           .flatMap( this::getDbToMemberEntries )
                                           .collect( Collectors.toMap( Pair::first, Pair::other ) );

        var allDatabases = memberLeaderMap.values().stream()
                                          .flatMap( Collection::stream )
                                          .collect( Collectors.toSet() );

        var leaderService = new StubLeaderService( dbToLeaderMap );

        var strategy = new RandomEvenStrategy( () -> allDatabases, leaderService, member3 );
        var validTopologies = allDatabases.stream()
                                          .map( db -> new TransferCandidates( db, Set.of( member1, member2, member3, member4 ) ) )
                                          .collect( Collectors.toList() );

        // when
        var target = strategy.select( validTopologies );

        // then
        assertThat( target.to() ).isEqualTo( member2 );
    }

    @Test
    void returnNoTargetIfTopologiesAndLowLeadershipMembersDontIntersect()
    {
        // given
        var member1 = new MemberId( UUID.randomUUID() );
        var member2 = new MemberId( UUID.randomUUID() );
        var member3 = new MemberId( UUID.randomUUID() );
        var member4 = new MemberId( UUID.randomUUID() );

        var memberLeaderMap = Map.of(
                member1, databaseIds( 0 ).limit( 4 ).collect( Collectors.toList() ),
                member2, databaseIds( 4 ).limit( 3 ).collect( Collectors.toList() ),
                member3, databaseIds( 7 ).limit( 5 ).collect( Collectors.toList() ),
                member4, databaseIds( 12 ).limit( 9 ).collect( Collectors.toList() ) );

        var dbToLeaderMap = memberLeaderMap.entrySet()
                                           .stream()
                                           .flatMap( this::getDbToMemberEntries )
                                           .collect( Collectors.toMap( Pair::first, Pair::other ) );

        var allDatabases = memberLeaderMap.values().stream()
                                          .flatMap( Collection::stream )
                                          .collect( Collectors.toSet() );

        var leaderService = new StubLeaderService( dbToLeaderMap );

        var strategy = new RandomEvenStrategy( () -> allDatabases, leaderService, member3 );
        var validTopologies = allDatabases.stream()
                                          .map( db -> new TransferCandidates( db, Set.of( member1, member3, member4 ) ) )
                                          .collect( Collectors.toList() );

        // when
        var target = strategy.select( validTopologies );

        // then
        assertThat( target ).isEqualTo( NO_TARGET );
    }

    Stream<NamedDatabaseId> databaseIds( int from )
    {
        return IntStream.iterate( from, i -> i + 1 )
                 .mapToObj( idx -> "database-" + idx )
                 .map( name -> DatabaseIdFactory.from( name, UUID.randomUUID() ) );
    }

    Stream<Pair<NamedDatabaseId,MemberId>> getDbToMemberEntries( Map.Entry<MemberId,List<NamedDatabaseId>> entry )
    {
        var member = entry.getKey();
        var dbs = entry.getValue();
        return dbs.stream().map( dbId -> Pair.of( dbId, member ) );
    }
}
