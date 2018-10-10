/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.akka.CommercialAkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemLifecycle;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemUncleanShutdownLifecycle;

import java.util.concurrent.ExecutorService;

import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

public class AkkaUncleanShutdownDiscoveryServiceFactory extends CommercialAkkaDiscoveryServiceFactory
{
    @Override
    protected ActorSystemLifecycle actorSystemLifecycle( Config config, ExecutorService executor, LogProvider logProvider, RemoteMembersResolver resolver )
    {
        return new ActorSystemUncleanShutdownLifecycle( actorSystemFactory( resolver, executor, config, logProvider ), logProvider );
    }
}
