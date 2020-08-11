/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.server.rest.causalclustering.ClusteringDatabaseService;
import com.neo4j.server.rest.causalclustering.ClusteringDbmsService;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.neo4j.configuration.GraphDatabaseSettings.Mode.CORE;
import static org.neo4j.configuration.GraphDatabaseSettings.Mode.READ_REPLICA;
import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIsBuilder;

public class EnterpriseDiscoverableURIs
{
    public static DiscoverableURIs enterpriseDiscoverableURIs( Config config, ConnectorPortRegister portRegister )
    {
        var discoverableURIsBuilder = communityDiscoverableURIsBuilder( config, portRegister );

        var mode = config.get( GraphDatabaseSettings.mode );
        if ( mode == CORE || mode == READ_REPLICA )
        {
            discoverableURIsBuilder.addEndpoint( ClusteringDatabaseService.KEY, ClusteringDatabaseService.absoluteDatabaseClusterPath( config ) );
            discoverableURIsBuilder.addEndpoint( ClusteringDbmsService.KEY, ClusteringDbmsService.absoluteDbmsClusterPath( config ) );
        }

        return discoverableURIsBuilder.build();
    }
}
