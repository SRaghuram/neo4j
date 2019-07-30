/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.discovery.DiscoverableURIs;

import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIsBuilder;

public class EnterpriseDiscoverableURIs
{
    public static DiscoverableURIs enterpriseDiscoverableURIs( Config config, ConnectorPortRegister portRegister )
    {
        return communityDiscoverableURIsBuilder( config, portRegister )
                .addEndpoint( "management", config.get( ServerSettings.management_api_path ).getPath() + "/" )
                .build();
    }
}
