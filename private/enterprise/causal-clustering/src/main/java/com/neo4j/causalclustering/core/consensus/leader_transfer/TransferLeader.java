/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.ServerGroupName;
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
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferTarget.NO_TARGET;
import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeadershipPriorityGroupSetting.READER;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

class TransferLeader implements Runnable
{
    private static final RandomStrategy PRIORITISED_SELECTION_STRATEGY = new RandomStrategy();
    private final Config config;
    private final Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler;
    private final MemberId myself;
    private final DatabasePenalties databasePenalties;
    private final SelectionStrategy selectionStrategy;
    private final RaftMembershipResolver membershipResolver;
    private final Set<ServerGroupName> myGroups;
    private final Supplier<List<NamedDatabaseId>> leadershipsResolver;

    TransferLeader( Config config, Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler, MemberId myself,
                    DatabasePenalties databasePenalties, SelectionStrategy leaderLoadBalancing, RaftMembershipResolver membershipResolver,
                    Supplier<List<NamedDatabaseId>> leadershipsResolver )
    {
        this.myGroups = myGroups( config );
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

    private LeaderTransferTarget createTarget( Collection<NamedDatabaseId> databaseIds, SelectionStrategy selectionStrategy )
    {
        if ( !databaseIds.isEmpty() )
        {
            var validTransferCandidatesPerDb = databaseIds.stream()
                                                          .flatMap( this::findValidTransferCandidatesForDb )
                                                          .collect( toList() );
            return selectionStrategy.select( validTransferCandidatesPerDb );
        }
        return NO_TARGET;
    }

    private Stream<TransferCandidates> findValidTransferCandidatesForDb( NamedDatabaseId namedDatabaseId )
    {
        var raftMembership = membershipResolver.membersFor( namedDatabaseId );
        var votingMembers = raftMembership.votingMembers();
        Predicate<MemberId> notSuspendedOrMe = member ->
                databasePenalties.notSuspended( namedDatabaseId.databaseId(), member ) &&
                !Objects.equals( member, myself );

        var validMembers = votingMembers.stream()
                                        .filter( notSuspendedOrMe )
                                        .collect( toSet() );

        if ( validMembers.isEmpty() )
        {
            return Stream.empty();
        }

        return Stream.of( new TransferCandidates( namedDatabaseId, validMembers ) );
    }

    private void handleProposal( LeaderTransferTarget transferTarget, Set<ServerGroupName> prioritisedGroups )
    {
        var raftId = RaftId.from( transferTarget.databaseId().databaseId() );
        var proposal = new Proposal( myself, transferTarget.to(), prioritisedGroups );
        var message = RaftMessages.InboundRaftMessageContainer.of( Instant.now(), raftId, proposal );
        messageHandler.handle( message );
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

    private static Set<ServerGroupName> myGroups( Config config )
    {
        return new HashSet<>( config.get( CausalClusteringSettings.server_groups ) );
    }

    private static Map<NamedDatabaseId,ServerGroupName> prioritisedGroups( Config config, List<NamedDatabaseId> existingDatabases )
    {
        var prioritisedGroupsPerDatabase = READER.read( config );
        return existingDatabases.stream()
                                .filter( db -> prioritisedGroupsPerDatabase.containsKey( db.name() ) )
                                .collect( toMap( identity(), db -> prioritisedGroupsPerDatabase.get( db.name() ) ) );
    }
}
