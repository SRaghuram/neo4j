/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.remote.artery.tcp.SSLEngineProvider;
import com.neo4j.causalclustering.discovery.AkkaDiscoverySSLEngineProvider;
import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.akka.system.ActorSystemFactory;

import java.util.Optional;

import org.neo4j.causalclustering.discovery.HostnameResolver;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;

public class CommercialAkkaDiscoveryServiceFactory extends BaseAkkaDiscoveryServiceFactory implements SslDiscoveryServiceFactory
{
    private Optional<SslPolicy> sslPolicy = Optional.empty();

    @Override
    protected ActorSystemFactory actorSystemFactory( HostnameResolver hostnameResolver, JobScheduler jobScheduler, Config config, LogProvider logProvider )
    {
        Optional<SSLEngineProvider> sslEngineProvider = sslPolicy.map( AkkaDiscoverySSLEngineProvider::new );
        return new ActorSystemFactory( hostnameResolver, sslEngineProvider, jobScheduler, config, logProvider );
    }

    @Override
    public void setSslPolicy( SslPolicy sslPolicy )
    {
        this.sslPolicy = Optional.ofNullable( sslPolicy );
    }
}
