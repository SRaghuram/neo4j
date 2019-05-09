/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.Address;
import akka.remote.ThisActorSystemQuarantinedEvent;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.causalclustering.discovery.MultiRetryStrategy;
import org.neo4j.causalclustering.discovery.RetryStrategy;
import org.neo4j.causalclustering.discovery.TopologyServiceNoRetriesStrategy;
import org.neo4j.causalclustering.discovery.TopologyServiceRetryStrategy;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class AkkaCoreTopologyServiceTest
{
    private Config config = Config.defaults();
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private LogProvider logProvider = NullLogProvider.getInstance();
    private LogProvider userLogProvider = NullLogProvider.getInstance();
    private TopologyServiceRetryStrategy catchupAddressretryStrategy = new TopologyServiceNoRetriesStrategy();
    private Clock clock = Clock.fixed( Instant.now(), ZoneId.of( "UTC" ) );
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private ActorSystemLifecycle system = mock( ActorSystemLifecycle.class, Answers.RETURNS_DEEP_STUBS );

    RetryStrategy<Void,Boolean> restartRetryStrategy =
            new MultiRetryStrategy<>( 0L, 10L, NullLogProvider.getInstance(), BaseAkkaDiscoveryServiceFactory::sleep );

    private AkkaCoreTopologyService service = new AkkaCoreTopologyService(
            config,
            myself,
            system,
            logProvider,
            userLogProvider,
            catchupAddressretryStrategy,
            restartRetryStrategy,
            executor,
            clock );

    private ThisActorSystemQuarantinedEvent event = ThisActorSystemQuarantinedEvent.apply(
            new Address( "protocol", "system", "host1", 1 ),
            new Address( "protocol", "system", "host2",2 ) );
    @Test
    public void shouldLifecycle() throws Throwable
    {
        service.init();
        service.start();

        verify( system ).createClusterActorSystem();
        verify( system, atLeastOnce() ).queueMostRecent( any() );
        verify( system, atLeastOnce() ).applicationActorOf( any(), any() );

        service.stop();
        service.shutdown();

        verify( system ).shutdown();
    }

    @Test
    public void shouldNotRestartIfPre()
    {
        service.restart();

        verifyZeroInteractions( system );
    }

    @Test
    public void shouldNotRestartIfIdle() throws Throwable
    {
        service.init();
        reset( system );

        service.restart();

        verifyZeroInteractions( system );
    }

    @Test
    public void shouldNotRestartIfHalt() throws Throwable
    {
        service.init();
        service.start();
        service.stop();
        service.shutdown();
        reset( system );

        service.restart();

        verifyZeroInteractions( system );
    }

    @Test
    public void shouldRestartIfRun() throws Throwable
    {
        service.init();
        service.start();
        reset( system );

        service.restart();

        InOrder inOrder = inOrder( system );
        inOrder.verify( system ).shutdown();
        inOrder.verify( system ).createClusterActorSystem();
    }

    @Test
    public void shouldRetryRestartIfStopFails() throws Throwable
    {
        service.init();
        service.start();
        reset( system );

        Mockito.doThrow( new RuntimeException() ).when( system ).shutdown();

        service.restart();

        InOrder inOrder = inOrder( system );
        inOrder.verify( system ).shutdown();
        inOrder.verify( system ).createClusterActorSystem();
    }

    @Test
    public void shouldRetryRestartIfStartFails() throws Throwable
    {
        service.init();
        service.start();
        reset( system );

        Mockito.doThrow( new RuntimeException() ).doNothing().when( system ).createClusterActorSystem();

        service.restart();

        InOrder inOrder = inOrder( system );
        inOrder.verify( system ).shutdown();
        inOrder.verify( system, times( 2 ) ).createClusterActorSystem();
    }
}
