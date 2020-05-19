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
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

public class LeaderTransferService extends LifecycleAdapter implements RejectedLeaderTransferHandler
{
    private final TransferLeaderJob transferLeaderJob;
    private final JobScheduler jobScheduler;
    private final Duration leaderTransferInterval;
    private final DatabasePenalties databasePenalties;
    private final TransferLeaderOnShutdown transferLeaderOnShutdown;
    private JobHandle<?> jobHandle;

    public LeaderTransferService( JobScheduler jobScheduler, Config config, Duration leaderTransferInterval,
                                  DatabaseManager<ClusteredDatabaseContext> databaseManager,
                                  Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler,
                                  MemberId myself, Duration leaderMemberBackoff, LogProvider logProvider, Clock clock )
    {
        this.databasePenalties = new DatabasePenalties( leaderMemberBackoff, clock );
        this.jobScheduler = jobScheduler;
        this.leaderTransferInterval = leaderTransferInterval;

        RaftMembershipResolver membershipResolver = id ->
                databaseManager.getDatabaseContext( id )
                               .map( ctx -> ctx.dependencies().resolveDependency( RaftMembership.class ) )
                               .orElse( RaftMembership.EMPTY );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, myself );
        this.transferLeaderJob = new TransferLeaderJob( config, messageHandler, myself,
                                                        databasePenalties, SelectionStrategy.NO_OP, membershipResolver, leadershipsResolver );
        this.transferLeaderOnShutdown = new TransferLeaderOnShutdown(
                config, messageHandler, myself, databasePenalties, membershipResolver, leadershipsResolver, logProvider );
    }

    @Override
    public void start() throws Exception
    {
        var schedulingTime = leaderTransferInterval.toMillis();
        jobHandle = jobScheduler.scheduleRecurring( Group.LEADER_TRANSFER_SERVICE, transferLeaderJob, schedulingTime, TimeUnit.MILLISECONDS );
    }

    @Override
    public void stop()
    {
        if ( jobHandle != null )
        {
            jobHandle.cancel();
        }
        transferLeaderOnShutdown.drainAllLeaderships();
    }

    @Override
    public void handleRejection( RaftMessages.LeadershipTransfer.Rejection rejection, NamedDatabaseId namedDatabaseId )
    {
        databasePenalties.issuePenalty( rejection.from(), namedDatabaseId );
    }

    @VisibleForTesting
    void awaitRunningJob()
    {
        if ( jobHandle == null )
        {
            return;
        }
        try
        {
            this.jobHandle.waitTermination();
            return;
        }
        catch ( CancellationException e )
        {
            return;
        }
        catch ( ExecutionException | InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
