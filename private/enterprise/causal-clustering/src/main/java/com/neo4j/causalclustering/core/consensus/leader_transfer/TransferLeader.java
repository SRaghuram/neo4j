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
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.configuration.ServerGroupName;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.time.Clock;
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
    protected final ServerGroupsSupplier myGroupsProvider;
    protected final Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler;
    protected final ClusteringIdentityModule identityModule;
    protected final DatabasePenalties databasePenalties;
    protected final RaftMembershipResolver membershipResolver;
    protected final Supplier<List<NamedDatabaseId>> leadershipsResolver;
    protected final Config config;
    private final Clock clock;

    public TransferLeader( ServerGroupsSupplier myGroupsProvider, Config config,
            Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler,
            ClusteringIdentityModule identityModule,DatabasePenalties databasePenalties, RaftMembershipResolver membershipResolver, Supplier<List<NamedDatabaseId>> leadershipsResolver,
            Clock clock )
    {
        this.myGroupsProvider = myGroupsProvider;
        this.messageHandler = messageHandler;
        this.identityModule = identityModule;
        this.databasePenalties = databasePenalties;
        this.membershipResolver = membershipResolver;
        this.leadershipsResolver = leadershipsResolver;
        this.config = config;
        this.clock = clock;
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
                !Objects.equals( member, identityModule.memberId( namedDatabaseId ) );

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
        var proposal =
                new RaftMessages.LeadershipTransfer.Proposal( identityModule.memberId( transferTarget.databaseId() ), transferTarget.to(), prioritisedGroups );
        var message = RaftMessages.InboundRaftMessageContainer.of( clock.instant(), raftId, proposal );
        messageHandler.handle( message );
    }
}
