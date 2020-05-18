/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.leader_transfer;

import com.neo4j.causalclustering.common.StubClusteredDatabaseManager;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.CallableExecutorService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.FakeClock;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.assertj.core.api.Assertions.assertThat;

class LeaderTransferServiceTest
{
    private JobScheduler jobScheduler;

    private final Config config = Config.newBuilder().build();
    private final MemberId myself = new MemberId( randomUUID() );
    private final MemberId core1 = new MemberId( randomUUID() );
    private final Duration leaderTransferInterval = Duration.ofSeconds( 5 );
    private final Duration leaderMemberBackoff = Duration.ofSeconds( 15 );

    @BeforeEach
    void startScheduler() throws Exception
    {
        assert jobScheduler == null;
        jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        jobScheduler.start();
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
        var messageHandler = new TransferLeaderTest.TrackingMessageHandler();
        var leaderService = new StubLeaderService( Map.of() );
        var leaderTransferService =
                new LeaderTransferService( jobScheduler, config, leaderTransferInterval, databaseManager, messageHandler, myself, leaderMemberBackoff,
                                           NullLogProvider.nullLogProvider(), clock, leaderService );

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
        var raftMembership = new TransferLeaderTest.StubRaftMembershipResolver( myself, core1 );
        var fooDb = databaseWithLeader( databaseManager, myself, "foo", raftMembership );
        var messageHandler = new TransferLeaderTest.TrackingMessageHandler();
        var leaderService = new StubLeaderService( Map.of( fooDb, myself ) );

        var leaderTransferService =
                new LeaderTransferService( jobScheduler, config, leaderTransferInterval, databaseManager, messageHandler, myself, leaderMemberBackoff,
                                           NullLogProvider.nullLogProvider(), clock, leaderService );

        // when
        life.add( leaderTransferService );
        life.start();
        life.shutdown();

        // then
        var jobHasFinished = runAsync( leaderTransferService::awaitRunningJob );
        assertThat( jobHasFinished ).succeedsWithin( Duration.ofSeconds( 30 ) );
        assertThat( messageHandler.proposals.stream().map( RaftMessages.InboundRaftMessageContainer::message ) )
                .containsExactly( new RaftMessages.LeadershipTransfer.Proposal( myself, core1, Set.of() ) );
    }

    private NamedDatabaseId databaseWithLeader( StubClusteredDatabaseManager databaseManager, MemberId member, String databaseName,
                                                TransferLeaderTest.StubRaftMembershipResolver raftMembershipResolver )
    {
        NamedDatabaseId dbId = DatabaseIdFactory.from( databaseName, UUID.randomUUID() );

        var dependencies = new Dependencies();
        dependencies.satisfyDependencies( raftMembershipResolver );
        dependencies.satisfyDependencies( raftMembershipResolver.membersFor( dbId ) );
        var leaderLocator = new RaftLeadershipsResolverTest.StubLeaderLocator( new LeaderInfo( member, 0 ) );
        databaseManager.givenDatabaseWithConfig()
                       .withDatabaseId( dbId )
                       .withDependencies( dependencies )
                       .withLeaderLocator( leaderLocator )
                       .register();
        return dbId;
    }
}
