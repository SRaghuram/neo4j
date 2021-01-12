/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.ServerGroupsSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.scheduler.CallableExecutorService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class LeaderTransferServiceTest
{
    private JobScheduler jobScheduler;

    private final Config config = Config.newBuilder().build();
    private final NamedDatabaseId databaseId = randomNamedDatabaseId();
    private final CoreServerIdentity myIdentity = new InMemoryCoreServerIdentity();
    private final RaftMemberId myRaftId = myIdentity.raftMemberId( databaseId );
    private final CoreServerIdentity remoteIdentity = new InMemoryCoreServerIdentity();
    private final RaftMemberId remoteRaftId = remoteIdentity.raftMemberId( databaseId );
    private final Duration leaderTransferInterval = Duration.ofSeconds( 5 );
    private final Duration leaderMemberBackoff = Duration.ofSeconds( 15 );
    private ServerGroupsSupplier serverGroupsProvider;

    @BeforeEach
    void startScheduler() throws Exception
    {
        assert jobScheduler == null;
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        jobScheduler.start();
        serverGroupsProvider = ServerGroupsSupplier.listen( config );
    }

    @AfterEach
    void stopScheduler() throws Exception
    {
        var executors = jobScheduler.activeGroups()
                .map( group -> (CallableExecutorService) jobScheduler.executor( group.group ) )
                .map( executor -> (ExecutorService) executor.delegate() )
                .collect( Collectors.toList() );
        jobScheduler.shutdown();
        assert executors.stream().allMatch( ExecutorService::isTerminated );
        jobScheduler = null;
    }

    @Test
    void shouldStartAndStop()
    {
        // given
        var clock = new FakeClock();
        var life = new LifeSupport();
        var databaseManager = new StubClusteredDatabaseManager();
        var messageHandler = new TransferLeaderJobTest.TrackingMessageHandler();
        var leaderService = new StubLeaderService( Map.of() );
        var leaderTransferService = new LeaderTransferService( jobScheduler, config, leaderTransferInterval, databaseManager, messageHandler, myIdentity,
                leaderMemberBackoff, nullLogProvider(), clock, leaderService, serverGroupsProvider, new StubRaftMembershipResolver() );

        // when
        life.add( leaderTransferService );
        life.start();
        life.shutdown();

        // then
        var jobHasFinished = runAsync( leaderTransferService::awaitRunningJob );
        assertThat( jobHasFinished ).succeedsWithin( Duration.ofSeconds( 30 ) );
    }

    @Test
    void shouldTransferLeaderOnStop()
    {
        // given
        var clock = new FakeClock();
        var life = new LifeSupport();
        var databaseManager = new StubClusteredDatabaseManager();
        var raftMembership = new StubRaftMembershipResolver( databaseId, myIdentity, remoteIdentity );
        databaseWithLeader( databaseId, databaseManager, myRaftId, raftMembership );
        var messageHandler = new TransferLeaderJobTest.TrackingMessageHandler();
        var leaderService = new StubLeaderService( Map.of( databaseId, myIdentity.serverId() ) );

        var leaderTransferService = new LeaderTransferService( jobScheduler, config, leaderTransferInterval, databaseManager, messageHandler, myIdentity,
                leaderMemberBackoff, nullLogProvider(), clock, leaderService, serverGroupsProvider, raftMembership );

        // when
        life.add( leaderTransferService );
        life.start();
        life.shutdown();

        // then
        var jobHasFinished = runAsync( leaderTransferService::awaitRunningJob );
        assertThat( jobHasFinished ).succeedsWithin( Duration.ofSeconds( 30 ) );
        assertThat( messageHandler.proposals.stream().map( RaftMessages.InboundRaftMessageContainer::message ) )
                .containsExactly( new RaftMessages.LeadershipTransfer.Proposal( myRaftId, remoteRaftId, Set.of() ) );
    }

    @Test
    void shouldHandleRejection()
    {
        // given
        var clock = new FakeClock();
        var databaseManager = new StubClusteredDatabaseManager();
        var raftMembership = new StubRaftMembershipResolver();
        databaseWithLeader( databaseId, databaseManager, myRaftId, raftMembership );
        var messageHandler = new TransferLeaderJobTest.TrackingMessageHandler();
        var leaderService = new StubLeaderService( Map.of( databaseId, myIdentity.serverId() ) );
        var logProvider = new AssertableLogProvider();

        var leaderTransferService = new LeaderTransferService( jobScheduler, config, leaderTransferInterval, databaseManager, messageHandler, myIdentity,
                leaderMemberBackoff, logProvider, clock, leaderService, serverGroupsProvider, raftMembership );
        var rejection = new RaftMessages.LeadershipTransfer.Rejection( remoteRaftId, 100, 1 );

        //when
        leaderTransferService.handleRejection( rejection, databaseId );

        //then
        assertThat( leaderTransferService.databasePenalties().notSuspended( databaseId.databaseId(), remoteIdentity.serverId() ) ).isTrue();
        LogAssertions.assertThat( logProvider ).forLevel( AssertableLogProvider.Level.WARN ).containsMessages(
                String.format( "LeaderTransferRequest rejected (%s) by %s whose ServerId is not present any more", databaseId, remoteRaftId ) );

        //when
        logProvider.clear();
        raftMembership.addReverseMapping( remoteIdentity.serverId(), remoteIdentity.raftMemberId( databaseId ) );
        leaderTransferService.handleRejection( rejection, databaseId );

        //then
        assertThat( leaderTransferService.databasePenalties().notSuspended( databaseId.databaseId(), remoteIdentity.serverId() ) ).isFalse();
        LogAssertions.assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    private NamedDatabaseId databaseWithLeader( NamedDatabaseId dbId, StubClusteredDatabaseManager databaseManager, RaftMemberId member,
            StubRaftMembershipResolver raftMembershipResolver )
    {
        var dependencies = new Dependencies();
        dependencies.satisfyDependencies( raftMembershipResolver );
        var leaderLocator = new RaftLeadershipsResolverTest.StubLeaderLocator( new LeaderInfo( member, 0 ) );
        databaseManager.givenDatabaseWithConfig()
                .withDatabaseId( dbId )
                .withDependencies( dependencies )
                .withLeaderLocator( leaderLocator )
                .register();
        return dbId;
    }
}
