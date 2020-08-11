/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import java.util.regex.Pattern;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.repr.OutputFormat;

@Path( ClusteringDatabaseService.DB_CLUSTER_PATH )
public class ClusteringDatabaseService extends AbstractClusteringDatabaseService
{
    private static final String CLUSTER_PATH = "/cluster";
    private static final String DB_NAME = "databaseName";
    static final String DB_CLUSTER_PATH = "/{" + DB_NAME + "}" + CLUSTER_PATH;
    public static final String KEY = "db" + CLUSTER_PATH;

    public ClusteringDatabaseService( @Context OutputFormat output, @Context DatabaseManagementService managementService,
                                     @Context DatabaseStateService databaseStateService, @PathParam( DB_NAME ) String databaseName )
    {
        super( output, databaseStateService, managementService, databaseName );
    }

    public static Pattern databaseClusterUriPattern( Config config )
    {
        return Pattern.compile( config.get( ServerSettings.db_api_path ).getPath() + "/[^/]*" + CLUSTER_PATH + ".*" );
    }

    public static String absoluteDatabaseClusterPath( Config config )
    {
        return config.get( ServerSettings.db_api_path ).getPath() + DB_CLUSTER_PATH;
    }

    @Override
    public String relativePath( String databaseName )
    {
        return databaseName + CLUSTER_PATH;
    }
}
