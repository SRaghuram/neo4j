/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.LeadershipTransfer.Proposal;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.Inbound;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferContext.NO_TARGET;
import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeadershipPriorityGroupSetting.READER;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

class TransferLeader implements Runnable
{
    private static final RandomStrategy PRIORITISED_SELECTION_STRATEGY = new RandomStrategy();
    private final Config config;
    private Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler;
    private MemberId myself;
    private DatabasePenalties databasePenalties;
    private SelectionStrategy selectionStrategy;
    private final RaftMembershipResolver membershipResolver;
    private final Supplier<List<NamedDatabaseId>> leadershipsResolver;

    TransferLeader( Config config, Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler, MemberId myself,
            DatabasePenalties databasePenalties, SelectionStrategy leaderLoadBalancing, RaftMembershipResolver membershipResolver,
            Supplier<List<NamedDatabaseId>> leadershipsResolver )
    {
        this.config = config;
        this.messageHandler = messageHandler;
        this.myself = myself;
        this.databasePenalties = databasePenalties;
        this.selectionStrategy = leaderLoadBalancing;
        this.membershipResolver = membershipResolver;
        this.leadershipsResolver = leadershipsResolver;
    }

    @Override
    public void run()
    {
        databasePenalties.clean();

        var prioritizedGroups = notPrioritisedLeadership();
        var leaderTransferContext = createContext( prioritizedGroups.keySet(), PRIORITISED_SELECTION_STRATEGY );

        if ( leaderTransferContext != NO_TARGET )
        {
            handleProposal( leaderTransferContext, Set.of( notPrioritisedLeadership().get( leaderTransferContext.databaseId() ) ) );
        }
        else
        {
            // This instance is high priority for all its leaderships, so just do normal load balancing
            leaderTransferContext = createContext( leadershipsResolver.get(), selectionStrategy );

            // If leaderTransferContext is still no target, there is no need to transfer leadership
            if ( leaderTransferContext != NO_TARGET )
            {
                handleProposal( leaderTransferContext, Set.of() );
            }
        }
    }

    private LeaderTransferContext createContext( Collection<NamedDatabaseId> databaseIds, SelectionStrategy selectionStrategy )
    {
        if ( !databaseIds.isEmpty() )
        {
            var validTopologies = databaseIds.stream()
                    .flatMap( this::filterValidMembers )
                    .collect( toList() );
            return selectionStrategy.select( validTopologies );
        }
        return NO_TARGET;
    }

    private Stream<TransferCandidates> filterValidMembers( NamedDatabaseId namedDatabaseId )
    {
        var raftMembership = membershipResolver.membersFor( namedDatabaseId );
        var votingMembers = raftMembership.votingMembers();

        var validMembers = votingMembers.stream()
                .filter( member -> databasePenalties.notSuspended( namedDatabaseId.databaseId(), member ) && !member.equals( myself ) )
                .collect( toSet() );

        if ( validMembers.isEmpty() )
        {
            return Stream.empty();
        }

        var raftId = RaftId.from( namedDatabaseId.databaseId() );

        return Stream.of( new TransferCandidates( namedDatabaseId, raftId, validMembers ) );
    }

    private void handleProposal( LeaderTransferContext transferContext, Set<String> prioritisedGroups )
    {
        var proposal = new Proposal( myself, transferContext.to(), prioritisedGroups );
        var message = RaftMessages.InboundRaftMessageContainer.of( Instant.now(), transferContext.raftId(), proposal );
        messageHandler.handle( message );
    }

    private Map<NamedDatabaseId,String> notPrioritisedLeadership()
    {
        Set<String> myGroups = getMyGroups( config );
        var namedDatabaseIds = leadershipsResolver.get();
        var prioritisedGroups = getPrioritisedGroups( config, namedDatabaseIds );
        if ( prioritisedGroups.isEmpty() )
        {
            return Map.of();
        }
        if ( myGroups.isEmpty() )
        {
            return prioritisedGroups;
        }
        var inPriorityLeadership = prioritisedGroups.entrySet()
                .stream()
                .filter( entry -> myGroups.contains( entry.getValue() ) )
                .map( Map.Entry::getKey )
                .collect( toList() );
        prioritisedGroups.keySet().removeAll( inPriorityLeadership );
        for ( NamedDatabaseId namedDatabaseId : prioritisedGroups.keySet() )
        {
            if ( myGroups.contains( prioritisedGroups.get( namedDatabaseId ) ) )
            {
                prioritisedGroups.remove( namedDatabaseId );
            }
        }
        return prioritisedGroups;
    }

    private Set<String> getMyGroups( Config config )
    {
        return new HashSet<>( config.get( CausalClusteringSettings.server_groups ) );
    }

    private Map<NamedDatabaseId,String> getPrioritisedGroups( Config config, List<NamedDatabaseId> existingDatabases )
    {
        var prioritisedGroupsPerDatabase = READER.read( config );
        return existingDatabases.stream()
                .filter( db -> prioritisedGroupsPerDatabase.containsKey( db.name() ) )
                .collect( toMap( identity(), db -> prioritisedGroupsPerDatabase.get( db.name() ) ) );
    }
}
