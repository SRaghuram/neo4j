/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorPath;
import akka.actor.ActorPaths;
import akka.actor.ActorSystem;
import akka.actor.BootstrapSetup;
import akka.actor.ProviderSelection;
import akka.actor.setup.ActorSystemSetup;
import akka.dispatch.ExecutionContexts;
import akka.remote.artery.tcp.SSLEngineProvider;
import akka.remote.artery.tcp.SSLEngineProviderSetup;
import scala.concurrent.ExecutionContextExecutor;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public class ActorSystemFactory
{
    public static final String ACTOR_SYSTEM_NAME = "cc-discovery-actor-system";
    private final JobScheduler jobScheduler;
    private final LogProvider logProvider;
    private final Optional<SSLEngineProvider> sslEngineProvider;
    private final TypesafeConfigService configService;
    private final int parallelism;

    public ActorSystemFactory( RemoteMembersResolver remoteMembersResolver, Optional<SSLEngineProvider> sslEngineProvider, JobScheduler jobScheduler,
            Config config, LogProvider logProvider )
    {
        this.jobScheduler = jobScheduler;
        this.logProvider = logProvider;
        this.sslEngineProvider = sslEngineProvider;
        TypesafeConfigService.ArteryTransport arteryTransport =
                sslEngineProvider.isPresent() ? TypesafeConfigService.ArteryTransport.TLS_TCP : TypesafeConfigService.ArteryTransport.TCP;
        this.configService = new TypesafeConfigService( remoteMembersResolver, arteryTransport, config );
        this.parallelism = config.get( CausalClusteringSettings.middleware_akka_default_parallelism_level );
    }

    Set<ActorPath> initialClientContacts()
    {
        return configService
                .initialActorSystemPaths()
                .stream()
                .map( systemPath -> ActorPaths.fromString( String.format( "%s/system/receptionist", systemPath ) ) )
                .collect( Collectors.toSet() );
    }

    ActorSystem createActorSystem( ProviderSelection providerSelection )
    {
        com.typesafe.config.Config tsConfig = configService.generate();

        ExecutionContextExecutor ec = ExecutionContexts.fromExecutor( jobScheduler.workStealingExecutorAsyncMode( Group.AKKA_TOPOLOGY_WORKER, parallelism ) );

        BootstrapSetup bootstrapSetup = BootstrapSetup.create( tsConfig )
                .withActorRefProvider( providerSelection )
                .withDefaultExecutionContext( ec );

        ActorSystemSetup actorSystemSetup = ActorSystemSetup.create( bootstrapSetup );

        if ( sslEngineProvider.isPresent() )
        {
            actorSystemSetup = actorSystemSetup.withSetup( SSLEngineProviderSetup.create( system -> sslEngineProvider.get() ) );
        }

        LoggingFilter.enable( logProvider );
        LoggingActor.enable( logProvider );
        ActorSystem actorSystem = ActorSystem.create( ACTOR_SYSTEM_NAME, actorSystemSetup );
        LoggingActor.enable( actorSystem, logProvider );
        return actorSystem;
    }
}
