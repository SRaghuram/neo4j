/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.client.config.ClientNetworkConfig;

import org.neo4j.causalclustering.discovery.HazelcastClientConnector;
import org.neo4j.causalclustering.discovery.HazelcastSslConfiguration;
import org.neo4j.causalclustering.discovery.HostnameResolver;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

public class SslHazelcastClientConnector extends HazelcastClientConnector
{
    private final SslPolicy sslPolicy;

    SslHazelcastClientConnector( Config config, LogProvider logProvider, SslPolicy sslPolicy,
                                 HostnameResolver hostnameResolver )
    {
        super( config, logProvider, hostnameResolver );
        this.sslPolicy = sslPolicy;
    }

    @Override
    protected void additionalConfig( ClientNetworkConfig networkConfig, LogProvider logProvider )
    {
        HazelcastSslConfiguration.configureSsl( networkConfig, sslPolicy, logProvider );
    }
}
