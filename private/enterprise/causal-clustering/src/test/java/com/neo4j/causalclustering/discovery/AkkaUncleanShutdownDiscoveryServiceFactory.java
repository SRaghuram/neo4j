/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemUncleanShutdownLifecycle;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.config.SslPolicyLoader;

public class AkkaUncleanShutdownDiscoveryServiceFactory extends AkkaDiscoveryServiceFactory
{
    @Override
    protected ActorSystemLifecycle actorSystemLifecycle( Config config, LogProvider logProvider, RemoteMembersResolver resolver,
                                                         SslPolicyLoader sslPolicyLoader,
                                                         DiscoveryFirstStartupDetector firstStartupDetector )
    {
        return new ActorSystemUncleanShutdownLifecycle(
                actorSystemFactory( sslPolicyLoader, firstStartupDetector, config, logProvider ),
                resolver,
                config,
                logProvider );
    }
}
