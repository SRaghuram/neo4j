/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.UUID;

import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.HostnameResolver;
import org.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import org.neo4j.causalclustering.discovery.TopologyServiceNoRetriesStrategy;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import org.neo4j.causalclustering.discovery.CoreTopologyService.Listener;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

public class CoreTopologyChangeListenerTest
{
    MemberId myself = new MemberId( UUID.randomUUID() );
    JobScheduler jobScheduler = new CentralJobScheduler();
    HostnameResolver hostnameResolver = new NoOpHostnameResolver();
    TopologyServiceRetryStrategy  topologyServiceRetryStrategy = new TopologyServiceNoRetriesStrategy();

    ActorSystemLifecycle actorSystemLifecycle = Mockito.mock( ActorSystemLifecycle.class );

    AkkaCoreTopologyService service = new AkkaCoreTopologyService(
            Config.defaults(),
            myself,
            actorSystemLifecycle,
            jobScheduler,
            NullLogProvider.getInstance(),
            NullLogProvider.getInstance(),
            hostnameResolver,
            topologyServiceRetryStrategy );

    @Test
    public void shouldNotifyListenersOnTopologyChange()
    {
        Listener listener = mock( Listener.class );
        service.addLocalCoreTopologyListener( listener );
        service.topologyState().onTopologyUpdate( CoreTopology.EMPTY );
        verify( listener ).onCoreTopologyChange( CoreTopology.EMPTY );
    }
}
