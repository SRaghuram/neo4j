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
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
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

import static com.neo4j.configuration.CausalClusteringSettings.leader_balancing;

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
                                  MemberId myself, Duration leaderMemberBackoff, LogProvider logProvider, Clock clock,
                                  LeaderService leaderService )
    {
        this.databasePenalties = new DatabasePenalties( leaderMemberBackoff, clock );
        this.jobScheduler = jobScheduler;
        this.leaderTransferInterval = leaderTransferInterval;

        RaftMembershipResolver membershipResolver = id ->
                databaseManager.getDatabaseContext( id )
                               .map( ctx -> ctx.dependencies().resolveDependency( RaftMembership.class ) )
                               .orElse( RaftMembership.EMPTY );

        var leadershipsResolver = new RaftLeadershipsResolver( databaseManager, myself );

        var nonPriorityStrategy = pickSelectionStrategy( config, databaseManager, leaderService, myself );

        this.transferLeaderJob = new TransferLeaderJob( config, messageHandler, myself,
                                                        databasePenalties, nonPriorityStrategy, membershipResolver, leadershipsResolver, clock );
        this.transferLeaderOnShutdown = new TransferLeaderOnShutdown(
                config, messageHandler, myself, databasePenalties, membershipResolver, leadershipsResolver, logProvider, clock );
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
        }
        catch ( CancellationException ignored )
        {
        }
        catch ( ExecutionException | InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static SelectionStrategy pickSelectionStrategy( Config config, DatabaseManager<ClusteredDatabaseContext> databaseManager,
                                                            LeaderService leaderService, MemberId myself )
    {
        var strategyChoice = config.get( leader_balancing );
        switch ( strategyChoice )
        {
        case EQUAL_BALANCING:
            return new RandomEvenStrategy( () -> databaseManager.registeredDatabases().keySet(), leaderService, myself );
        case NO_BALANCING:
        default:
            return SelectionStrategy.NO_OP;
        }
    }
}
