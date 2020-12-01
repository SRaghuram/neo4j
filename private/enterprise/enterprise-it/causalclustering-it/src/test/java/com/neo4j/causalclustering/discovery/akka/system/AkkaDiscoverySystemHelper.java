/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.InitialDiscoveryMembersResolver;
import com.neo4j.causalclustering.discovery.NoOpHostnameResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TestFirstStartupDetector;
import com.neo4j.causalclustering.discovery.akka.DummyPanicService;
import com.neo4j.causalclustering.discovery.member.TestCoreDiscoveryMember;
import com.neo4j.causalclustering.identity.InMemoryCoreServerIdentity;
import com.neo4j.dbms.EnterpriseDatabaseState;

import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.StubDatabaseStateService;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.logging.Level;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.test.ports.PortAuthority;
import org.neo4j.time.Clocks;

import static com.neo4j.configuration.CausalClusteringSettings.discovery_advertised_address;
import static com.neo4j.configuration.CausalClusteringSettings.discovery_listen_address;
import static com.neo4j.configuration.CausalClusteringSettings.initial_discovery_members;
import static com.neo4j.configuration.CausalClusteringSettings.middleware_logging_level;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.neo4j.configuration.GraphDatabaseSettings.SERVER_DEFAULTS;
import static org.neo4j.configuration.GraphDatabaseSettings.store_internal_log_level;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createInitialisedScheduler;

public class AkkaDiscoverySystemHelper
{
    static final NamedDatabaseId NAMED_DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();

    static CoreTopologyService coreTopologyService( int harnessPort, DiscoveryServiceFactory discoveryServiceFactory )
    {
        return coreTopologyService( harnessPort, discoveryServiceFactory, PortAuthority.allocatePort() );
    }

    static CoreTopologyService coreTopologyService( int harnessPort, DiscoveryServiceFactory discoveryServiceFactory, int discoPort )
    {
        return coreTopologyService( harnessPort, discoveryServiceFactory, discoPort, Set.of( NAMED_DATABASE_ID ) );
    }

    static CoreTopologyService coreTopologyService( int harnessPort, DiscoveryServiceFactory discoveryServiceFactory, int discoPort,
            Set<NamedDatabaseId> startedDatabases )
    {
        var discoverySocket = new SocketAddress( "localhost", discoPort );
        var config = Config.newBuilder()
                           .setDefaults( SERVER_DEFAULTS )
                           .set( discovery_listen_address, discoverySocket )
                           .set( discovery_advertised_address, discoverySocket )
                           .set( initial_discovery_members, singletonList( new SocketAddress( "localhost" , harnessPort ) ) )
                           .set( store_internal_log_level, Level.DEBUG )
                           .set( middleware_logging_level, Level.DEBUG )
                           .build();
        var identityModule = new InMemoryCoreServerIdentity();
        var logProvider = NullLogProvider.getInstance();
        var membersResolver = new InitialDiscoveryMembersResolver( new NoOpHostnameResolver(), config );
        var monitors = new Monitors();
        var retryStrategy = new RetryStrategy( 100, 3 );
        var sslPolicyLoader = SslPolicyLoader.create( config, logProvider );
        var firstStartupDetector = new TestFirstStartupDetector( true );
        Function<NamedDatabaseId,DatabaseState> asStarted = id -> new EnterpriseDatabaseState( id, STARTED );
        var databaseStates = startedDatabases.stream().collect( Collectors.toMap( identity(), asStarted ) );
        var databaseStateService = new StubDatabaseStateService( databaseStates, EnterpriseDatabaseState::unknown );
        var panicker = DummyPanicService.PANICKER;

        return discoveryServiceFactory.coreTopologyService( config, identityModule, createInitialisedScheduler(), logProvider,
                                                            logProvider, membersResolver, retryStrategy, sslPolicyLoader, TestCoreDiscoveryMember::factory,
                                                            firstStartupDetector, monitors, Clocks.systemClock(), databaseStateService, panicker );
    }
}
