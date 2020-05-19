/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferTarget.NO_TARGET;
import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeadershipPriorityGroupSetting.READER;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

class TransferLeaderJob extends TransferLeader implements Runnable
{
    private static final RandomStrategy PRIORITISED_SELECTION_STRATEGY = new RandomStrategy();
    private final SelectionStrategy selectionStrategy;

    TransferLeaderJob( Config config, Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler, MemberId myself,
                       DatabasePenalties databasePenalties, SelectionStrategy leaderLoadBalancing, RaftMembershipResolver membershipResolver,
                       Supplier<List<NamedDatabaseId>> leadershipsResolver )
    {
        super( config, messageHandler, myself, databasePenalties, membershipResolver, leadershipsResolver );
        this.selectionStrategy = leaderLoadBalancing;
    }

    @Override
    public void run()
    {
        var myLeaderships = leadershipsResolver.get();

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
        var leaderTransferTarget = createTarget( undesiredLeaderships.keySet(), PRIORITISED_SELECTION_STRATEGY );

        if ( leaderTransferTarget == NO_TARGET )
        {
            return false;
        }

        handleProposal( leaderTransferTarget, Set.of( undesiredLeaderships.get( leaderTransferTarget.databaseId() ) ) );
        return true;
    }

    private void doBalancing( List<NamedDatabaseId> leaderships )
    {
        var unPrioritisedLeaderships = unPrioritisedLeaderships( config, leaderships );
        var leaderTransferTarget = createTarget( unPrioritisedLeaderships, selectionStrategy );

        if ( leaderTransferTarget != NO_TARGET )
        {
            handleProposal( leaderTransferTarget, Set.of() );
        }
    }

    private Map<NamedDatabaseId,ServerGroupName> undesiredLeaderships( List<NamedDatabaseId> myLeaderships )
    {
        var prioritisedGroups = prioritisedGroups( config, myLeaderships );

        if ( prioritisedGroups.isEmpty() )
        {
            return Map.of();
        }
        if ( myGroups.isEmpty() )
        {
            return prioritisedGroups;
        }

        prioritisedGroups.entrySet().removeIf( entry -> myGroups.contains( entry.getValue() ) );

        return prioritisedGroups;
    }

    private static List<NamedDatabaseId> unPrioritisedLeaderships( Config config, List<NamedDatabaseId> myLeaderships )
    {
        Set<NamedDatabaseId> leadershipsWithPriorityGroups = prioritisedGroups( config, myLeaderships ).keySet();
        return myLeaderships.stream()
                            .filter( db -> !leadershipsWithPriorityGroups.contains( db ) )
                            .collect( Collectors.toList() );
    }

    private static Map<NamedDatabaseId,ServerGroupName> prioritisedGroups( Config config, List<NamedDatabaseId> existingDatabases )
    {
        var prioritisedGroupsPerDatabase = READER.read( config );
        return existingDatabases.stream()
                                .filter( db -> prioritisedGroupsPerDatabase.containsKey( db.name() ) )
                                .collect( toMap( identity(), db -> prioritisedGroupsPerDatabase.get( db.name() ) ) );
    }
}
