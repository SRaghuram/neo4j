/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.configuration.EnterpriseEditionSettings;
import com.neo4j.server.rest.causalclustering.ClusteringDatabaseService;
import com.neo4j.server.rest.causalclustering.ClusteringDbmsService;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIsBuilder;

public class EnterpriseDiscoverableURIs
{
    public static DiscoverableURIs enterpriseDiscoverableURIs( Config config, ConnectorPortRegister portRegister )
    {
        var discoverableURIsBuilder = communityDiscoverableURIsBuilder( config, portRegister );

        var enabledModules = config.get( ServerSettings.http_enabled_modules );
        if ( enabledModules.contains( ConfigurableServerModules.ENTERPRISE_MANAGEMENT_ENDPOINTS ) && modeAllowsStatusEndpoint( config ) )
        {
            discoverableURIsBuilder.addEndpoint( ClusteringDatabaseService.KEY, ClusteringDatabaseService.absoluteDatabaseClusterPath( config ) );
            discoverableURIsBuilder.addEndpoint( ClusteringDbmsService.KEY, ClusteringDbmsService.absoluteDbmsClusterPath( config ) );
        }

        return discoverableURIsBuilder.build();
    }

    private static boolean modeAllowsStatusEndpoint( Config config )
    {
        var mode = config.get( GraphDatabaseSettings.mode );
        switch ( mode )
        {
        case CORE:
        case READ_REPLICA:
            return true;
        case SINGLE:
            return config.get( EnterpriseEditionSettings.enable_clustering_in_standalone );
        default:
            throw new IllegalStateException( "Invalid mode " + mode );
        }
    }
}
