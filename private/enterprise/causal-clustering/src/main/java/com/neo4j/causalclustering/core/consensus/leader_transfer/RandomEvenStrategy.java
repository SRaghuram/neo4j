/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.StrategyUtils.selectRandom;

public class RandomEvenStrategy implements SelectionStrategy
{
    private final Supplier<Set<NamedDatabaseId>> databasesSupplier;
    private final CoreServerIdentity myIdentity;
    private final LeaderService leaderService;

    RandomEvenStrategy( Supplier<Set<NamedDatabaseId>> databasesSupplier, LeaderService leaderService, CoreServerIdentity myIdentity )
    {
        this.leaderService = leaderService;
        this.databasesSupplier = databasesSupplier;
        this.myIdentity = myIdentity;
    }

    @Override
    public LeaderTransferTarget select( List<TransferCandidates> validTopologies )
    {
        if ( validTopologies.isEmpty() )
        {
            return LeaderTransferTarget.NO_TARGET;
        }

        var allServers = validTopologies.stream()
                                        .flatMap( t -> t.members().stream() )
                                        .collect( Collectors.toSet() );

        var validServers = serversWithFewerLeaderships( allServers );
        var targetTopologies = validTopologies.stream()
                       .filter( t -> !Collections.disjoint( validServers, t.members() ) )
                       .collect( Collectors.toList() );

        var randomTopology = selectRandom( targetTopologies );

        var randomTarget = randomTopology.flatMap( t ->
        {
            var validMembersInTopology = findValidMembersForTopology( t, validServers );
            return selectRandom( validMembersInTopology ).map( serverId -> new LeaderTransferTarget( t.databaseId(), serverId ) );
        } );

        return randomTarget.orElse( LeaderTransferTarget.NO_TARGET );
    }

    private Set<ServerId> findValidMembersForTopology( TransferCandidates targetTopology, Set<ServerId> validMembers )
    {
        return targetTopology.members().stream().filter( validMembers::contains ).collect( Collectors.toSet() );
    }

    /**
     * Valid targets for membership transfer must have >= 2 fewer leaderships than this instance. If
     * we allowed transfers between instances whose leadership counts were just 1 apart, this would
     * lead to leadership "ping-pong" whenever {@code $number_of_databases % $number_of_members != 0}.
     *
     * @return returns a set of cluster members with *more* than one fewer leaderships than this instance
     */
    private Set<ServerId> serversWithFewerLeaderships( Set<ServerId> allServers )
    {
        var databaseIds = databasesSupplier.get();
        Map<ServerId,Long> leadershipCounts = databaseIds.stream()
                   .flatMap( dbId -> leaderService.getLeaderId( dbId ).stream() )
                   .collect( Collectors.groupingBy( Function.identity(), Collectors.counting() ) );

        allServers.forEach( serverId -> leadershipCounts.putIfAbsent( serverId, 0L ) );

        var myLeadershipCount = leadershipCounts.getOrDefault( myIdentity.serverId(), 0L );

        return leadershipCounts.entrySet().stream()
                               .filter( e -> e.getValue() < ( myLeadershipCount - 1 ) )
                               .map( Map.Entry::getKey )
                               .collect( Collectors.toSet() );
    }
}
