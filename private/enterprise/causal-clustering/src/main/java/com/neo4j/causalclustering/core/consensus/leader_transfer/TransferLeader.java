/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferTarget.NO_TARGET;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public abstract class TransferLeader
{
    protected final Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler;
    protected final MemberId myself;
    protected final DatabasePenalties databasePenalties;
    protected final RaftMembershipResolver membershipResolver;
    protected final Set<ServerGroupName> myGroups;
    protected final Supplier<List<NamedDatabaseId>> leadershipsResolver;
    protected final Config config;

    public TransferLeader( Config config, Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler, MemberId myself,
                           DatabasePenalties databasePenalties, RaftMembershipResolver membershipResolver, Supplier<List<NamedDatabaseId>> leadershipsResolver )
    {
        this.messageHandler = messageHandler;
        this.myself = myself;
        this.databasePenalties = databasePenalties;
        this.membershipResolver = membershipResolver;
        this.leadershipsResolver = leadershipsResolver;

        this.myGroups = myGroups( config );
        this.config = config;
    }

    protected LeaderTransferTarget createTarget( Collection<NamedDatabaseId> databaseIds, SelectionStrategy selectionStrategy )
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

    protected void handleProposal( LeaderTransferTarget transferTarget, Set<ServerGroupName> prioritisedGroups )
    {
        var raftId = RaftId.from( transferTarget.databaseId().databaseId() );
        var proposal = new RaftMessages.LeadershipTransfer.Proposal( myself, transferTarget.to(), prioritisedGroups );
        var message = RaftMessages.InboundRaftMessageContainer.of( Instant.now(), raftId, proposal );
        messageHandler.handle( message );
    }

    private static Set<ServerGroupName> myGroups( Config config )
    {
        return Set.copyOf( config.get( CausalClusteringSettings.server_groups ) );
    }
}
