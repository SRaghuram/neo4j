/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeadershipPriorityGroupSetting.READER;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toMap;

class TransferLeaderJob implements Runnable
{
    private static final RandomStrategy RANDOMISED_SELECTION_STRATEGY = new RandomStrategy();
    private final LeadershipTransferor leadershipTransferor;
    private final SelectionStrategy selectionStrategy;
    private final ServerGroupsSupplier myServerGroups;
    private final Config config;
    private final Supplier<List<NamedDatabaseId>> leadershipsResolver;
    private final ReadOnlyDatabaseChecker readOnlyDatabaseChecker;

    TransferLeaderJob( LeadershipTransferor leadershipTransferor, ServerGroupsSupplier myServerGroups, Config config,
            SelectionStrategy leaderLoadBalancing, Supplier<List<NamedDatabaseId>> leadershipsResolver )
    {
        this.myServerGroups = myServerGroups;
        this.config = config;
        this.leadershipsResolver = leadershipsResolver;
        this.leadershipTransferor = leadershipTransferor;
        this.selectionStrategy = leaderLoadBalancing;
        this.readOnlyDatabaseChecker = new ReadOnlyDatabaseChecker.Default( config );
    }

    @Override
    public void run()
    {
        var myLeaderships = leadershipsResolver.get();

        if ( transferLeadershipIfReadOnly( myLeaderships ) )
        {
            return;
        }

        // We only balance leadership for one database at a time,
        //   and we give priority to balancing according to prioritised groups over other strategies
        if ( doPrioritisedBalancing( myLeaderships ) )
        {
            return;
        }

        // This instance is high priority for all its leaderships, so just do normal load balancing for all other databases
        doBalancing( myLeaderships );
    }

    private boolean doPrioritisedBalancing( List<NamedDatabaseId> leaderships )
    {
        var undesiredLeaderships = undesiredLeaderships( leaderships );
        return leadershipTransferor.toPrioritisedGroup( undesiredLeaderships.keySet(), RANDOMISED_SELECTION_STRATEGY,
                                                        databaseId -> Set.of( undesiredLeaderships.get( databaseId ) ) );
    }

    private void doBalancing( List<NamedDatabaseId> leaderships )
    {
        var dbsWithPriorityGroups = prioritisedGroups( config, leaderships ).keySet();
        var unPrioritisedNonSystemLeaderships = leaderships.stream()
                                                           .filter( not( dbsWithPriorityGroups::contains ) )
                                                           .filter( not( NamedDatabaseId::isSystemDatabase ) )
                                                           .collect( Collectors.toList() );
        leadershipTransferor.balanceLeadership( unPrioritisedNonSystemLeaderships, selectionStrategy );
    }

    private Map<NamedDatabaseId,ServerGroupName> undesiredLeaderships( List<NamedDatabaseId> myLeaderships )
    {
        var myCurrentGroups = myServerGroups.get();
        var prioritisedGroups = prioritisedGroups( config, myLeaderships );

        if ( prioritisedGroups.isEmpty() )
        {
            return Map.of();
        }
        if ( myCurrentGroups.isEmpty() )
        {
            return prioritisedGroups;
        }

        prioritisedGroups.entrySet().removeIf( entry -> myCurrentGroups.contains( entry.getValue() ) );

        return prioritisedGroups;
    }

    private static Map<NamedDatabaseId,ServerGroupName> prioritisedGroups( Config config, List<NamedDatabaseId> myLeaderships )
    {
        var prioritisedGroupsPerDatabase = READER.read( config );
        var defaultPriorityGroup = config.get( CausalClusteringSettings.default_leadership_priority_group );
        if ( Objects.equals( defaultPriorityGroup, ServerGroupName.EMPTY ) && prioritisedGroupsPerDatabase.isEmpty() )
        {
            return Map.of();
        }
        return myLeaderships.stream()
                            .map( dbId -> Pair.of( dbId, prioritisedGroupsPerDatabase.getOrDefault( dbId.name(), defaultPriorityGroup ) ) )
                            .filter( pair -> !Objects.equals( pair.other(), ServerGroupName.EMPTY ) )
                            .collect( toMap( Pair::first, Pair::other ) );
    }

    private boolean transferLeadershipIfReadOnly( List<NamedDatabaseId> myLeaderships )
    {
        var readOnlyLeaderships = myLeaderships.stream()
                                               .filter( db -> readOnlyDatabaseChecker.test( db.name() ) )
                                               .collect( Collectors.toList() );
        return leadershipTransferor.balanceLeadership( readOnlyLeaderships, RANDOMISED_SELECTION_STRATEGY );
    }
}
