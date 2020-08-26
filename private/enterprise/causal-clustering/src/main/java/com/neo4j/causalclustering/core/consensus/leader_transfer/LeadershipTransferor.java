/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.configuration.ServerGroupName;

import java.time.Clock;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferTarget.NO_TARGET;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

class LeadershipTransferor
{
    private final Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler;
    private final ClusteringIdentityModule identityModule;
    private final DatabasePenalties databasePenalties;
    private final RaftMembershipResolver membershipResolver;
    private final Clock clock;
    private final Function<RaftMemberId,ServerId> serverIdResolver;

    LeadershipTransferor( Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler,
            ClusteringIdentityModule identityModule,DatabasePenalties databasePenalties, RaftMembershipResolver membershipResolver, Clock clock,
            Function<RaftMemberId,ServerId> serverIdResolver )
    {
        this.messageHandler = messageHandler;
        this.identityModule = identityModule;
        this.databasePenalties = databasePenalties;
        this.membershipResolver = membershipResolver;
        this.clock = clock;
        this.serverIdResolver = serverIdResolver;
    }

    boolean toPrioritisedGroup( Collection<NamedDatabaseId> undesiredLeaders, SelectionStrategy selectionStrategy,
            Function<NamedDatabaseId,Set<ServerGroupName>> prioritisedGroupsProvider )
    {
        var target = createTarget( undesiredLeaders, selectionStrategy );
        if ( target == NO_TARGET )
        {
            return false;
        }
        var targetDatabaseId = target.databaseId();
        var prioritisedGroups = prioritisedGroupsProvider.apply( targetDatabaseId );
        if ( prioritisedGroups.isEmpty() )
        {
            throw new IllegalArgumentException( "Server groups must exists for database " + targetDatabaseId + " when sending to prioritised groups." );
        }
        handleProposal( target, prioritisedGroups );
        return true;
    }

    boolean balanceLeadership( Collection<NamedDatabaseId> leaders, SelectionStrategy selectionStrategy )
    {
        var target = createTarget( leaders, selectionStrategy );
        if ( target == NO_TARGET )
        {
            return false;
        }
        handleProposal( target, Set.of() );
        return true;
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
        Predicate<RaftMemberId> notSuspendedOrMe = member ->
                !Objects.equals( member, identityModule.memberId( namedDatabaseId ) ) &&
                databasePenalties.notSuspended( namedDatabaseId.databaseId(), serverIdResolver.apply( member ) );

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
        var proposal = new RaftMessages.LeadershipTransfer.Proposal(
                identityModule.memberId( transferTarget.databaseId() ), transferTarget.to(), prioritisedGroups );
        var message = RaftMessages.InboundRaftMessageContainer.of( clock.instant(), raftId, proposal );
        messageHandler.handle( message );
    }
}
