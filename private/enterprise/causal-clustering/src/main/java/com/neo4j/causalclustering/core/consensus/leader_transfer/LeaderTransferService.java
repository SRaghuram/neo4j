/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembership;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.configuration.ServerGroupsSupplier;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

import static com.neo4j.configuration.CausalClusteringSettings.leader_balancing;

public class LeaderTransferService extends LifecycleAdapter implements RejectedLeaderTransferHandler
{
    private static final RandomStrategy DRAIN_SELECTION_STRATEGY = new RandomStrategy();

    private final TransferLeaderJob transferLeaderJob;
    private final JobScheduler jobScheduler;
    private final Duration leaderTransferInterval;
    private final DatabasePenalties databasePenalties;
    private final Log log;
    private final Function<RaftMemberId,ServerId> serverIdResolver;
    private JobHandle<?> jobHandle;
    private final RaftLeadershipsResolver leadershipsResolver;
    private final LeadershipTransferor leadershipTransferor;

    public LeaderTransferService( JobScheduler jobScheduler, Config config, Duration leaderTransferInterval,
            DatabaseManager<ClusteredDatabaseContext> databaseManager,
            Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> messageHandler,
            ClusteringIdentityModule identityModule, Duration leaderMemberBackoff, LogProvider logProvider, Clock clock,
            LeaderService leaderService, ServerGroupsSupplier serverGroupsSupplier, Function<RaftMemberId,ServerId> serverIdResolver )
    {
        this.databasePenalties = new DatabasePenalties( leaderMemberBackoff, clock );
        this.jobScheduler = jobScheduler;
        this.leaderTransferInterval = leaderTransferInterval;
        this.log = logProvider.getLog( getClass() );
        this.serverIdResolver = serverIdResolver;

        RaftMembershipResolver membershipResolver = id ->
                databaseManager.getDatabaseContext( id )
                        .map( ctx -> ctx.dependencies().resolveDependency( RaftMembership.class ) )
                        .orElse( RaftMembership.EMPTY );

        this.leadershipTransferor = new LeadershipTransferor( messageHandler, identityModule, databasePenalties, membershipResolver, clock, serverIdResolver );
        this.leadershipsResolver = new RaftLeadershipsResolver( databaseManager, identityModule );

        this.transferLeaderJob = new TransferLeaderJob( leadershipTransferor, serverGroupsSupplier, config,
                pickSelectionStrategy( config, databaseManager, leaderService, identityModule, serverIdResolver ), leadershipsResolver );
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
        drainAllLeaderships();
    }

    private void drainAllLeaderships()
    {
        var myLeaderships = leadershipsResolver.get();

        for ( var leadership : myLeaderships )
        {
            if ( !leadershipTransferor.balanceLeadership( List.of( leadership ), DRAIN_SELECTION_STRATEGY ) )
            {
                log.info( "Unable to attempt leadership transfer, no suitable member found for database: %s", leadership.name() );
            }
        }
    }

    @Override
    public void handleRejection( RaftMessages.LeadershipTransfer.Rejection rejection, NamedDatabaseId namedDatabaseId )
    {
        databasePenalties.issuePenalty( serverIdResolver.apply( rejection.from() ), namedDatabaseId );
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
                                                            LeaderService leaderService, ClusteringIdentityModule identityModule,
                                                            Function<RaftMemberId,ServerId> serverIdResolver )
    {
        var strategyChoice = config.get( leader_balancing );
        switch ( strategyChoice )
        {
        case EQUAL_BALANCING:
            return new RandomEvenStrategy( () -> databaseManager.registeredDatabases().keySet(), leaderService, identityModule, serverIdResolver );
        case NO_BALANCING:
        default:
            return SelectionStrategy.NO_OP;
        }
    }
}
