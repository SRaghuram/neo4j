/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DiscoveryMember;
import com.neo4j.causalclustering.discovery.NoRetriesStrategy;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TestDiscoveryMember;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoreTopologyChangeListenerTest
{
    private final DatabaseId databaseId = new DatabaseId( "my_db" );
    private final DiscoveryMember myself = new TestDiscoveryMember( Set.of( databaseId ) );
    private final RetryStrategy catchupAddressRetryStrategy = new NoRetriesStrategy();
    private final RetryStrategy discoveryRestartRetryStrategy = new NoRetriesStrategy();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final ActorSystemLifecycle actorSystemLifecycle = Mockito.mock( ActorSystemLifecycle.class );

    private final AkkaCoreTopologyService service = new AkkaCoreTopologyService(
            Config.defaults(),
            myself,
            actorSystemLifecycle,
            NullLogProvider.getInstance(),
            NullLogProvider.getInstance(),
            catchupAddressRetryStrategy,
            discoveryRestartRetryStrategy,
            executor,
            Clocks.systemClock() );

    @Test
    void shouldNotifyListenersOnTopologyChange()
    {
        DatabaseCoreTopology coreTopology = new DatabaseCoreTopology( databaseId, new RaftId( UUID.randomUUID() ), Map.of() );
        Listener listener = mock( Listener.class );
        when( listener.databaseId() ).thenReturn( databaseId );
        service.addLocalCoreTopologyListener( listener );
        service.topologyState().onTopologyUpdate( coreTopology );
        verify( listener ).onCoreTopologyChange( coreTopology );
    }
}
