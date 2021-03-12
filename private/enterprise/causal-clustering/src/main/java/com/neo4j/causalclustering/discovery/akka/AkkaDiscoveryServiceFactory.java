/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.remote.artery.tcp.SSLEngineProvider;
import com.neo4j.causalclustering.discovery.AkkaDiscoverySSLEngineProvider;
import com.neo4j.causalclustering.discovery.DiscoveryFirstStartupDetector;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import com.neo4j.causalclustering.discovery.RetryStrategy;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.discovery.member.DefaultServerSnapshot;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.CoreServerIdentity;
import com.neo4j.configuration.MinFormationMembers;

import java.time.Clock;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

public class AkkaDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    @Override
    public final AkkaCoreTopologyService coreTopologyService( Config config, CoreServerIdentity myIdentity, JobScheduler jobScheduler, LogProvider logProvider,
                                                              LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
                                                              RetryStrategy catchupAddressRetryStrategy, SslPolicyLoader sslPolicyLoader,
                                                              ServerSnapshotFactory serverSnapshotFactory, DiscoveryFirstStartupDetector firstStartupDetector,
                                                              Monitors monitors, Clock clock, DatabaseStateService databaseStateService, Panicker panicker )
    {
        ActorSystemRestarter actorSystemRestarter = ActorSystemRestarter.forConfig( config );
        var minFormationMembers = MinFormationMembers.from( config );
        var akkaActorSystemRestartStrategy = new AkkaActorSystemRestartStrategy.RestartWhenMajorityUnreachableOrSingletonFirstSeed( remoteMembersResolver );

        var actorSystem = actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, firstStartupDetector, minFormationMembers,
                                                akkaActorSystemRestartStrategy );
        return new AkkaCoreTopologyService( config, myIdentity, actorSystem, logProvider, userLogProvider, catchupAddressRetryStrategy, actorSystemRestarter,
                                            serverSnapshotFactory, jobScheduler, clock, monitors, databaseStateService, panicker
        );
    }

    @Override
    public final AkkaTopologyClient readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, ServerIdentity myIdentity,
                                                                RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader,
                                                                ServerSnapshotFactory serverSnapshotFactory, Clock clock,
                                                                DatabaseStateService databaseStateService )
    {
        var firstStartupDetector = new ReadReplicaDiscoveryFirstStartupDetector();
        var minFormationMembers = MinFormationMembers.from( config );
        var akkaActorSystemRestartStrategy = new AkkaActorSystemRestartStrategy.NeverRestart();

        var actorSystemLifecycle = actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, firstStartupDetector, minFormationMembers,
                                                         akkaActorSystemRestartStrategy );
        return new AkkaTopologyClient( config, logProvider, myIdentity, actorSystemLifecycle, DefaultServerSnapshot::rrSnapshot, clock, jobScheduler,
                                       databaseStateService );
    }

    @Override
    public TopologyService standaloneTopologyService( Config config, ServerIdentity myIdentity, JobScheduler jobScheduler, LogProvider logProvider,
                                                      LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
                                                      RetryStrategy topologyServiceRetryStrategy, SslPolicyLoader sslPolicyLoader,
                                                      ServerSnapshotFactory serverSnapshotFactory, Monitors monitors, Clock clock,
                                                      DatabaseStateService databaseStateService, Panicker panicker )
    {
        ActorSystemRestarter actorSystemRestarter = ActorSystemRestarter.forConfig( config );
        var minFormationMembers = new MinFormationMembers( 1 );
        var akkaActorSystemRestartStrategy = new AkkaActorSystemRestartStrategy.NeverRestart();

        var actorSystemLifecycle = actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, () -> true, minFormationMembers,
                                                         akkaActorSystemRestartStrategy );
        return new AkkaStandaloneTopologyService( config, myIdentity, actorSystemLifecycle, logProvider, userLogProvider, topologyServiceRetryStrategy,
                                                  actorSystemRestarter, serverSnapshotFactory, jobScheduler, clock, monitors, databaseStateService, panicker );
    }

    protected ActorSystemLifecycle actorSystemLifecycle( Config config, LogProvider logProvider, RemoteMembersResolver resolver,
                                                         SslPolicyLoader sslPolicyLoader, DiscoveryFirstStartupDetector firstStartupDetector,
                                                         MinFormationMembers minFormationMembers,
                                                         AkkaActorSystemRestartStrategy akkaActorSystemRestartStrategy )
    {
        var actorSystemFactory = actorSystemFactory( sslPolicyLoader, firstStartupDetector, config, logProvider, minFormationMembers );
        return new ActorSystemLifecycle( actorSystemFactory, resolver, new JoinMessageFactory( resolver ), config, logProvider, minFormationMembers,
                                         akkaActorSystemRestartStrategy );
    }

    protected static ActorSystemFactory actorSystemFactory( SslPolicyLoader sslPolicyLoader, DiscoveryFirstStartupDetector firstStartupDetector, Config config,
                                                            LogProvider logProvider, MinFormationMembers minFormationMembers )
    {
        SslPolicy sslPolicy = sslPolicyLoader.hasPolicyForSource( CLUSTER ) ? sslPolicyLoader.getPolicy( CLUSTER ) : null;
        Optional<SSLEngineProvider> sslEngineProvider = Optional.ofNullable( sslPolicy ).map( AkkaDiscoverySSLEngineProvider::new );
        return new ActorSystemFactory( sslEngineProvider, firstStartupDetector, config, logProvider, minFormationMembers );
    }
}
