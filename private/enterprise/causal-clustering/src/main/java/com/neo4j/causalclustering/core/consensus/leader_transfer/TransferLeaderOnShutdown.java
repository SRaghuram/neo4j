/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;

import java.time.Clock;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferTarget.NO_TARGET;

class TransferLeaderOnShutdown extends TransferLeader
{
    private static final RandomStrategy DRAIN_SELECTION_STRATEGY = new RandomStrategy();
    private final Log log;

    TransferLeaderOnShutdown( Config config, Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler, MemberId myself,
                              DatabasePenalties databasePenalties, RaftMembershipResolver membershipResolver,
                              Supplier<List<NamedDatabaseId>> leadershipsResolver, LogProvider logProvider, Clock clock )
    {
        super( config, messageHandler, myself, databasePenalties, membershipResolver, leadershipsResolver, clock );
        this.log = logProvider.getLog( this.getClass() );
    }

    public void drainAllLeaderships()
    {
        var myLeaderships = leadershipsResolver.get();

        for ( var leadership : myLeaderships )
        {
            var leaderTransferTarget = createTarget( List.of( leadership ), DRAIN_SELECTION_STRATEGY );
            if ( leaderTransferTarget == NO_TARGET )
            {
                log.info( "Unable to attempt leadership transfer, no suitable member found for database: %s", leadership.name() );
            }
            else
            {
                handleProposal( leaderTransferTarget, Set.of() );
            }
        }
    }
}
