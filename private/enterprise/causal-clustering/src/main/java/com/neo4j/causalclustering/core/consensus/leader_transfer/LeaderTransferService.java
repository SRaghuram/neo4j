/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

public class LeaderTransferService extends LifecycleAdapter implements RejectedLeaderTransferHandler
{
    private final TransferLeader transferLeader;
    private final JobScheduler jobScheduler;
    private final Duration leaderTransferInterval;
    private final DatabasePenalties databasePenalties;
    private JobHandle<?> jobHandle;

    public LeaderTransferService( JobScheduler jobScheduler, Duration leaderTransferInterval, Config config,
            DatabaseManager<ClusteredDatabaseContext> databaseManager, Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler,
            MemberId myself, Duration leaderMemberBackoff, Clock clock )
    {
        this.databasePenalties = new DatabasePenalties( leaderMemberBackoff, clock );
        this.jobScheduler = jobScheduler;
        this.leaderTransferInterval = leaderTransferInterval;

        RaftMembershipResolver membershipResolver = id ->
                databaseManager.getDatabaseContext( id )
                               .map( ctx -> ctx.dependencies().resolveDependency( RaftMembership.class ) )
                               .orElse( RaftMembership.EMPTY );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, myself );
        this.transferLeader = new TransferLeader( config, messageHandler, myself,
                                                  databasePenalties, SelectionStrategy.NO_OP, membershipResolver, leadershipsResolver );
    }

    @Override
    public void start() throws Exception
    {
        var schedulingTime = leaderTransferInterval.toMillis();
        jobHandle = jobScheduler.scheduleRecurring( Group.LEADER_TRANSFER_SERICE, transferLeader, schedulingTime, TimeUnit.MILLISECONDS );
    }

    @Override
    public void stop()
    {
        if ( jobHandle != null )
        {
            jobHandle.cancel();
        }
    }

    @Override
    public void handleRejection( RaftMessages.LeadershipTransfer.Rejection rejection, NamedDatabaseId namedDatabaseId )
    {
        databasePenalties.issuePenalty( rejection.from(), namedDatabaseId );
    }
}
