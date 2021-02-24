/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.member.TestCoreServerSnapshot;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;
import org.neo4j.time.Clocks;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreTopologyChangeListenerTest
{
    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final CoreServerIdentity myIdentity = new InMemoryCoreServerIdentity();
    private final RetryStrategy catchupAddressRetryStrategy = new NoRetriesStrategy();
    private final ActorSystemRestarter actorSystemRestarter = ActorSystemRestarter.forTest( 0 );
    private final JobScheduler jobScheduler = new ThreadPoolJobScheduler( Executors.newSingleThreadExecutor() );
    private final DatabaseStateService databaseStateService = new StubDatabaseStateService( dbId -> new EnterpriseDatabaseState( dbId, STARTED ) );

    private final ActorSystemLifecycle actorSystemLifecycle = Mockito.mock( ActorSystemLifecycle.class );

    private final AkkaCoreTopologyService service = new AkkaCoreTopologyService(
            Config.defaults(),
            myIdentity,
            actorSystemLifecycle,
            NullLogProvider.getInstance(),
            NullLogProvider.getInstance(),
            catchupAddressRetryStrategy,
            actorSystemRestarter,
            TestCoreServerSnapshot.factory( myIdentity ),
            jobScheduler,
            Clocks.systemClock(),
            new Monitors(),
            databaseStateService,
            DummyPanicService.PANICKER
    );

    @Test
    void shouldNotifyListenersOnTopologyChange()
    {
        var coreTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), IdFactory.randomRaftId(), Map.of() );
        var listener = mock( Listener.class );
        when( listener.namedDatabaseId() ).thenReturn( namedDatabaseId );
        service.addLocalCoreTopologyListener( listener );
        service.topologyState().onTopologyUpdate( coreTopology );
        verify( listener, times( 2 ) ).onCoreTopologyChange( coreTopology.resolve( service.topologyState()::resolveRaftMemberForServer ) );
    }
}
