/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.StrategyUtils.selectRandom;
import static java.util.function.Function.identity;

public class RandomEvenStrategy implements SelectionStrategy
{
    private final Supplier<Set<NamedDatabaseId>> databasesSupplier;
    private final ClusteringIdentityModule identityModule;
    private final LeaderService leaderService;

    RandomEvenStrategy( Supplier<Set<NamedDatabaseId>> databasesSupplier, LeaderService leaderService, ClusteringIdentityModule identityModule )
    {
        this.leaderService = leaderService;
        this.databasesSupplier = databasesSupplier;
        this.identityModule = identityModule;
    }

    @Override
    public LeaderTransferTarget select( List<TransferCandidates> validTopologies )
    {
        if ( validTopologies.isEmpty() )
        {
            return LeaderTransferTarget.NO_TARGET;
        }

        var allMembers = validTopologies.stream()
                                        .flatMap( t -> t.members().stream() )
                                        .collect( Collectors.toSet() );

        var validMembers = membersWithFewerLeaderships( allMembers );
        var targetTopologies = validTopologies.stream()
                       .filter( t -> !Collections.disjoint( validMembers, t.members() ) )
                       .collect( Collectors.toList() );

        var randomTopology = selectRandom( targetTopologies );

        var randomTarget = randomTopology.flatMap( t ->
        {
            var validMembersInTopology = findValidMembersForTopology( t, validMembers );
            return selectRandom( validMembersInTopology ).map( m -> new LeaderTransferTarget( t.databaseId(), m ) );
        } );

        return randomTarget.orElse( LeaderTransferTarget.NO_TARGET );
    }

    private Set<MemberId> findValidMembersForTopology( TransferCandidates targetTopology, Set<MemberId> validMembers )
    {
        var memberIntersection = new HashSet<>( targetTopology.members() );
        memberIntersection.retainAll( validMembers );
        return memberIntersection;
    }

    /**
     * Valid targets for membership transfer must have >= 2 fewer leaderships than this instance. If
     * we allowed transfers between instances whose leadership counts were just 1 apart, this would
     * lead to leadership "ping-pong" whenever {@code $number_of_databases % $number_of_members != 0}.
     *
     * @return returns a set of cluster members with *more* than one fewer leaderships than this instance
     */
    private Set<MemberId> membersWithFewerLeaderships( Set<MemberId> allMembers )
    {
        var databaseIds = databasesSupplier.get();
        var leadershipCounts = databaseIds.stream()
                   .flatMap( dbId -> leaderService.getLeaderId( dbId ).stream() )
                   .collect( Collectors.groupingBy( identity(), Collectors.counting() ) );

        allMembers.forEach( member -> leadershipCounts.putIfAbsent( member, 0L ) );

        var myLeadershipCount = leadershipCounts.getOrDefault( identityModule.memberId(), 0L );

        return leadershipCounts.entrySet().stream()
                               .filter( e -> e.getValue() < ( myLeadershipCount - 1 ) )
                               .map( Map.Entry::getKey )
                               .collect( Collectors.toSet() );
    }
}
