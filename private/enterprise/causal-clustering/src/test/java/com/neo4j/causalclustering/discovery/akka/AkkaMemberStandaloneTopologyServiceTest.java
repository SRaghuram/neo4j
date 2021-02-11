/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.TestCoreServerSnapshot;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.scheduler.JobSchedulerAdapter;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class AkkaMemberStandaloneTopologyServiceTest
{
    private Config config = Config.defaults();
    private CoreServerIdentity myIdentity = new InMemoryCoreServerIdentity();
    private LogProvider logProvider = NullLogProvider.getInstance();
    private LogProvider userLogProvider = NullLogProvider.getInstance();
    private RetryStrategy catchupAddressretryStrategy = new NoRetriesStrategy();
    private Clock clock = Clock.fixed( Instant.now(), ZoneId.of( "UTC" ) );
    private final int maxAkkaSystemRestartAttempts = 3;
    private ActorSystemRestarter actorSystemRestarter = ActorSystemRestarter.forTest( maxAkkaSystemRestartAttempts );

    private Map<NamedDatabaseId,DatabaseState> databaseStates;
    private DatabaseStateService databaseStateService;
    private ActorSystem system;
    private ActorSystemLifecycle systemLifecycle;
    private TestKit testKit;
    private AkkaMemberStandaloneTopologyService service;
    private NamedDatabaseId databaseId1;
    private NamedDatabaseId databaseId2;
    private RaftMemberId raftMemberId;

    @BeforeEach
    void setup()
    {
        databaseId1 = randomNamedDatabaseId();
        databaseId2 = randomNamedDatabaseId();
        databaseStates = Map.of(
                databaseId1, new EnterpriseDatabaseState( databaseId1, STARTED ),
                databaseId2, new EnterpriseDatabaseState( databaseId2, STOPPED ) );
        databaseStateService = new StubDatabaseStateService( databaseStates, EnterpriseDatabaseState::unknown );

        system = ActorSystem.create();
        systemLifecycle = mock( ActorSystemLifecycle.class );
        testKit = new TestKit( system );

        when( systemLifecycle.applicationActorOf( any( Props.class ), anyString() ) )
                .then( i ->
                       {
                           var name = i.getArgument( 1, String.class );
                           return testKit.getRef();
                       } );

        service = new AkkaMemberStandaloneTopologyService(
                config,
                myIdentity,
                systemLifecycle,
                logProvider,
                userLogProvider,
                catchupAddressretryStrategy,
                actorSystemRestarter,
                TestCoreServerSnapshot.factory( myIdentity ),
                new JobSchedulerAdapter(),
                clock,
                new Monitors(),
                databaseStateService,
                DummyPanicService.PANICKER
        );
        raftMemberId = new RaftMemberId( myIdentity.serverId().uuid() );
    }

    @AfterEach
    void tearDown()
    {
        TestKit.shutdownActorSystem( system );
        system = null;
    }

    @Test
    void snapshotSetsStandaloneAsLeaderToAllDatabases()
    {
        var serverSnapshot = service.createServerSnapshot();
        assertThat( serverSnapshot.discoverableDatabases() ).containsOnly( databaseId1.databaseId() );
        assertThat( serverSnapshot.databaseLeaderships().values() )
                .containsOnly( new LeaderInfo( raftMemberId, 1 ) )
                .hasSize( 2 );
    }
}
