/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TestDiscoveryMember;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.StubClusteringIdentityModule;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.ConstantTimeTimeoutStrategy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreTopologyChangeListenerTest
{
    private final NamedDatabaseId namedDatabaseId = TestDatabaseIdRepository.randomNamedDatabaseId();
    private final ClusteringIdentityModule identityModule = new StubClusteringIdentityModule();
    private final RetryStrategy catchupAddressRetryStrategy = new NoRetriesStrategy();
    private final Restarter restarter = new Restarter( new ConstantTimeTimeoutStrategy( 1, MILLISECONDS ), 0 );
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ActorSystemLifecycle actorSystemLifecycle = Mockito.mock( ActorSystemLifecycle.class );

    private final AkkaCoreTopologyService service = new AkkaCoreTopologyService(
            Config.defaults(),
            identityModule,
            actorSystemLifecycle,
            NullLogProvider.getInstance(),
            NullLogProvider.getInstance(),
            catchupAddressRetryStrategy,
            restarter,
            TestDiscoveryMember::new,
            executor,
            Clocks.systemClock(),
            new Monitors() );

    @Test
    void shouldNotifyListenersOnTopologyChange()
    {
        var coreTopology = new DatabaseCoreTopology( namedDatabaseId.databaseId(), IdFactory.randomRaftId(), Map.of() );
        var listener = mock( Listener.class );
        when( listener.namedDatabaseId() ).thenReturn( namedDatabaseId );
        service.addLocalCoreTopologyListener( listener );
        service.topologyState().onTopologyUpdate( coreTopology );
        verify( listener, times( 2 ) ).onCoreTopologyChange( coreTopology.members( service.topologyState()::resolveRaftMemberForServer ) );
    }
}
