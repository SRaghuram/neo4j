/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.client.config.ClientNetworkConfig;

import org.neo4j.causalclustering.discovery.HazelcastClientConnector;
import org.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

public class SslHazelcastClientConnector extends HazelcastClientConnector
{
    private final SslPolicy sslPolicy;

    SslHazelcastClientConnector( Config config, LogProvider logProvider, SslPolicy sslPolicy, RemoteMembersResolver remoteMembersResolver )
    {
        super( config, logProvider, remoteMembersResolver );
        this.sslPolicy = sslPolicy;
    }

    @Override
    protected void additionalConfig( ClientNetworkConfig networkConfig, LogProvider logProvider )
    {
        HazelcastSslConfiguration.configureSsl( networkConfig, sslPolicy, logProvider );
    }
}
