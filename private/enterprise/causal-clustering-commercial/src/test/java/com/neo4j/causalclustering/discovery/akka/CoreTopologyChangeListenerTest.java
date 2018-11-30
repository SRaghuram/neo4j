/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import org.neo4j.causalclustering.discovery.NoRetriesStrategy;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.time.Clocks;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CoreTopologyChangeListenerTest
{
    MemberId myself = new MemberId( UUID.randomUUID() );
    RetryStrategy  topologyServiceRetryStrategy = new NoRetriesStrategy();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    ActorSystemLifecycle actorSystemLifecycle = Mockito.mock( ActorSystemLifecycle.class );

    AkkaCoreTopologyService service = new AkkaCoreTopologyService(
            Config.defaults(),
            myself,
            actorSystemLifecycle,
            NullLogProvider.getInstance(),
            NullLogProvider.getInstance(),
            topologyServiceRetryStrategy,
            executor,
            Clocks.systemClock() );

    @Test
    public void shouldNotifyListenersOnTopologyChange()
    {
        Listener listener = mock( Listener.class );
        service.addLocalCoreTopologyListener( listener );
        service.topologyState().onTopologyUpdate( CoreTopology.EMPTY );
        verify( listener ).onCoreTopologyChange( CoreTopology.EMPTY );
    }
}
