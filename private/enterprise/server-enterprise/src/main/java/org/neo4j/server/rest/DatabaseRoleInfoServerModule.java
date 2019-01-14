/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest;

import java.net.URI;
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.causalclustering.CausalClusteringService;
import org.neo4j.server.web.WebServer;

import static java.util.Arrays.asList;

public class DatabaseRoleInfoServerModule implements ServerModule
{
    private final WebServer server;
    private final Config config;
    private final Log log;

    public DatabaseRoleInfoServerModule( WebServer server, Config config, LogProvider logProvider )
    {
        this.server = server;
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        URI baseUri = managementApiUri();
        server.addJAXRSClasses( getClassNames(), baseUri.toString(), null );

        log.info( "Mounted REST API at: %s", baseUri.toString() );
    }

    @Override
    public void stop()
    {
        URI baseUri = managementApiUri();
        server.removeJAXRSClasses( getClassNames(), baseUri.toString() );
    }

    private List<String> getClassNames()
    {
        return asList(
                MasterInfoService.class.getName(),
                CoreDatabaseAvailabilityService.class.getName(),
                ReadReplicaDatabaseAvailabilityService.class.getName(),
                CausalClusteringService.class.getName()
        );
    }

    private URI managementApiUri()
    {
        return config.get( ServerSettings.management_api_path );
    }
}
