/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.server.rest.causalclustering.CausalClusteringService;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.web.WebServer;

public class DatabaseRoleInfoServerModule implements ServerModule
{
    private final WebServer server;
    private final Config config;

    public DatabaseRoleInfoServerModule( WebServer server, Config config )
    {
        this.server = server;
        this.config = config;
    }

    @Override
    public void start()
    {
        var mountPoint = mountPoint();
        server.addJAXRSClasses( jaxRsClasses(), mountPoint, null );
    }

    @Override
    public void stop()
    {
        server.removeJAXRSClasses( jaxRsClasses(), mountPoint() );
    }

    private String mountPoint()
    {
        return config.get( ServerSettings.db_api_path ).toString();
    }

    private static List<Class<?>> jaxRsClasses()
    {
        return List.of( CausalClusteringService.class );
    }
}
