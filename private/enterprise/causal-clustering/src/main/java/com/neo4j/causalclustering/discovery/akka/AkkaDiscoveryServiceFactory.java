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
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.JoinMessageFactory;
import com.neo4j.causalclustering.discovery.member.CoreServerSnapshotFactory;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.CoreServerIdentity;

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
    public final AkkaCoreTopologyService coreTopologyService( Config config, CoreServerIdentity myIdentity, JobScheduler jobScheduler,
                                                              LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
                                                              RetryStrategy catchupAddressRetryStrategy,
                                                              SslPolicyLoader sslPolicyLoader, CoreServerSnapshotFactory serverSnapshotFactory,
                                                              DiscoveryFirstStartupDetector firstStartupDetector,
                                                              Monitors monitors, Clock clock, DatabaseStateService databaseStateService,
                                                              Panicker panicker )
    {
        ActorSystemRestarter actorSystemRestarter = ActorSystemRestarter.forConfig( config );

        return new AkkaCoreTopologyService(
                config,
                myIdentity,
                actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, firstStartupDetector ),
                logProvider,
                userLogProvider,
                catchupAddressRetryStrategy,
                actorSystemRestarter,
                serverSnapshotFactory,
                jobScheduler,
                clock,
                monitors,
                databaseStateService,
                panicker
        );
    }

    @Override
    public final AkkaTopologyClient readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
            ServerIdentity myIdentity, RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader,
            ServerSnapshotFactory serverSnapshotFactory, Clock clock, DatabaseStateService databaseStateService )
    {
        return new AkkaTopologyClient(
                config,
                logProvider,
                myIdentity,
                actorSystemLifecycle( config, logProvider, remoteMembersResolver, sslPolicyLoader, new ReadReplicaDiscoveryFirstStartupDetector() ),
                serverSnapshotFactory,
                clock,
                jobScheduler,
                databaseStateService );
    }

    protected ActorSystemLifecycle actorSystemLifecycle( Config config, LogProvider logProvider, RemoteMembersResolver resolver,
                                                         SslPolicyLoader sslPolicyLoader,
                                                         DiscoveryFirstStartupDetector firstStartupDetector )
    {
        return new ActorSystemLifecycle(
                actorSystemFactory( sslPolicyLoader, firstStartupDetector, config, logProvider ),
                resolver,
                new JoinMessageFactory( resolver ),
                config,
                logProvider );
    }

    protected static ActorSystemFactory actorSystemFactory( SslPolicyLoader sslPolicyLoader, DiscoveryFirstStartupDetector firstStartupDetector,
                                                            Config config, LogProvider logProvider )
    {
        SslPolicy sslPolicy = sslPolicyLoader.hasPolicyForSource( CLUSTER ) ? sslPolicyLoader.getPolicy( CLUSTER ) : null;
        Optional<SSLEngineProvider> sslEngineProvider = Optional.ofNullable( sslPolicy ).map( AkkaDiscoverySSLEngineProvider::new );
        return new ActorSystemFactory( sslEngineProvider, firstStartupDetector, config, logProvider );
    }
}
