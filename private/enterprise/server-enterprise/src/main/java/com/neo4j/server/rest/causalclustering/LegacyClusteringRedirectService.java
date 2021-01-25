/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import java.util.regex.Pattern;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.OutputFormat;

import static com.neo4j.server.rest.causalclustering.LegacyClusteringRedirectService.CC_PATH;

@Path( CC_PATH )
public class LegacyClusteringRedirectService extends AbstractClusteringDatabaseService
{
    static final String CC_PATH = "/server/causalclustering";

    public LegacyClusteringRedirectService( @Context OutputFormat output, @Context DatabaseManagementService managementService,
                                           @Context DatabaseStateService dbStateService, @Context Config config )
    {
        super( output, dbStateService, managementService, config.get( GraphDatabaseSettings.default_database ) );
    }

    public static Pattern databaseLegacyClusterUriPattern( Config config )
    {
        return Pattern.compile( config.get( ServerSettings.management_api_path ).getPath() + CC_PATH + ".*" );
    }

    @Override
    public String relativePath( String databaseName )
    {
        return CC_PATH;
    }
}
