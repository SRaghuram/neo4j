/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import com.neo4j.server.rest.causalclustering.CausalClusteringService;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode.CORE;
import static com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.Mode.READ_REPLICA;
import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIsBuilder;

public class EnterpriseDiscoverableURIs
{
    public static DiscoverableURIs enterpriseDiscoverableURIs( Config config, ConnectorPortRegister portRegister )
    {
        var discoverableURIsBuilder = communityDiscoverableURIsBuilder( config, portRegister );

        var mode = config.get( EnterpriseEditionSettings.mode );
        if ( mode == CORE || mode == READ_REPLICA )
        {
            discoverableURIsBuilder.addEndpoint( CausalClusteringService.NAME, CausalClusteringService.absoluteDatabaseClusterPath( config ) );
        }

        return discoverableURIsBuilder.build();
    }
}
