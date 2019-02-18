/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;

import org.neo4j.configuration.Config;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.LogProvider;

public class HazelcastClientConnector implements HazelcastConnector
{
    private final Config config;
    private final LogProvider logProvider;
    private final RemoteMembersResolver remoteMembersResolver;

    public HazelcastClientConnector( Config config, LogProvider logProvider, RemoteMembersResolver remoteMembersResolver )
    {
        this.config = config;
        this.logProvider = logProvider;
        this.remoteMembersResolver = remoteMembersResolver;
    }

    @Override
    public HazelcastInstance connectToHazelcast()
    {
        ClientConfig clientConfig = new ClientConfig();

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        remoteMembersResolver
                .resolve( AdvertisedSocketAddress::toString )
                .forEach( networkConfig::addAddress );

        additionalConfig( networkConfig, logProvider );

        return HazelcastClient.newHazelcastClient( clientConfig );
    }

    protected void additionalConfig( ClientNetworkConfig networkConfig, LogProvider logProvider )
    {

    }
}
