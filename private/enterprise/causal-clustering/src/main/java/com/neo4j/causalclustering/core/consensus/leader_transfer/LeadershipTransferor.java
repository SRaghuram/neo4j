/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.configuration.ServerGroupName;

import java.time.Clock;
import java.util.Collection;
import java.util.HashSet;
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
    private final CoreServerIdentity myIdentity;
    private final DatabasePenalties databasePenalties;
    private final RaftMembershipResolver membershipResolver;
    private final Clock clock;

    LeadershipTransferor( Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler, CoreServerIdentity myIdentity,
            DatabasePenalties databasePenalties, RaftMembershipResolver membershipResolver, Clock clock )
    {
        this.messageHandler = messageHandler;
        this.myIdentity = myIdentity;
        this.databasePenalties = databasePenalties;
        this.membershipResolver = membershipResolver;
        this.clock = clock;
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
        return handleProposal( target, prioritisedGroups );
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
        var votingServers = new HashSet<>( membershipResolver.votingServers( namedDatabaseId ) );

        Predicate<ServerId> notSuspendedOrMe = serverId ->
                !Objects.equals( serverId, myIdentity.serverId() ) &&
                databasePenalties.notSuspended( namedDatabaseId.databaseId(), serverId );

        var validServers = votingServers.stream()
                .filter( notSuspendedOrMe )
                .collect( toSet() );

        if ( validServers.isEmpty() )
        {
            return Stream.empty();
        }

        return Stream.of( new TransferCandidates( namedDatabaseId, validServers ) );
    }

    private boolean handleProposal( LeaderTransferTarget transferTarget, Set<ServerGroupName> prioritisedGroups )
    {
        var raftGroupId = RaftGroupId.from( transferTarget.databaseId().databaseId() );
        final RaftMemberId from = myIdentity.raftMemberId( transferTarget.databaseId() );
        final RaftMemberId to = membershipResolver.resolveRaftMemberForServer( transferTarget.databaseId(), transferTarget.to() );
        if ( to == null )
        {
            return false;
        }
        var proposal = new RaftMessages.LeadershipTransfer.Proposal( from, to, prioritisedGroups );
        var message = RaftMessages.InboundRaftMessageContainer.of( clock.instant(), raftGroupId, proposal );
        messageHandler.handle( message );
        return true;
    }
}
