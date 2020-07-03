/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import akka.actor.ActorSystem;
import akka.actor.BootstrapSetup;
import akka.actor.ProviderSelection;
import akka.actor.setup.ActorSystemSetup;
import akka.remote.artery.tcp.SSLEngineProvider;
import akka.remote.artery.tcp.SSLEngineProviderSetup;
import com.typesafe.config.ConfigRenderOptions;

import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ActorSystemFactory
{
    public static final String ACTOR_SYSTEM_NAME = "cc-discovery-actor-system";
    private final LogProvider logProvider;
    private final Optional<SSLEngineProvider> sslEngineProvider;
    private final TypesafeConfigService configService;
    private final Log log;

    public ActorSystemFactory( Optional<SSLEngineProvider> sslEngineProvider, Config config, LogProvider logProvider )
    {
        this.logProvider = logProvider;
        this.sslEngineProvider = sslEngineProvider;
        TypesafeConfigService.ArteryTransport arteryTransport =
                sslEngineProvider.isPresent() ? TypesafeConfigService.ArteryTransport.TLS_TCP : TypesafeConfigService.ArteryTransport.TCP;
        this.configService = new TypesafeConfigService( arteryTransport, config );
        this.log = logProvider.getLog( getClass() );
    }

    ActorSystem createActorSystem( ProviderSelection providerSelection )
    {
        com.typesafe.config.Config tsConfig = configService.generate();
        log.debug( "Akka config: " + tsConfig.root().render( ConfigRenderOptions.concise() ) );

        BootstrapSetup bootstrapSetup = BootstrapSetup.create( tsConfig )
                .withActorRefProvider( providerSelection );

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
