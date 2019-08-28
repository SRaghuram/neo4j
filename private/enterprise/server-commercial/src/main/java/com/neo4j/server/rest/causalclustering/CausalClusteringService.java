/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.DatabaseService;
import org.neo4j.server.rest.repr.OutputFormat;

@Path( CausalClusteringService.DB_CLUSTER_PATH )
public class CausalClusteringService
{
    public static final String NAME = "cluster";
    private static final String CLUSTER_PATH = "/" + NAME;
    private static final String DB_NAME = "databaseName";
    static final String DB_CLUSTER_PATH = "/{" + DB_NAME + "}" + CLUSTER_PATH;

    static final String AVAILABLE = "available";
    static final String WRITABLE = "writable";
    static final String READ_ONLY = "read-only";
    static final String STATUS = "status";

    private final CausalClusteringStatus status;

    public CausalClusteringService( @Context OutputFormat output, @Context DatabaseService dbService, @PathParam( DB_NAME ) String databaseName )
    {
        this.status = CausalClusteringStatusFactory.build( output, dbService, databaseName );
    }

    @GET
    public Response discover()
    {
        return status.discover();
    }

    @GET
    @Path( WRITABLE )
    public Response isWritable()
    {
        return status.writable();
    }

    @GET
    @Path( READ_ONLY )
    public Response isReadOnly()
    {
        return status.readonly();
    }

    @GET
    @Path( AVAILABLE )
    public Response isAvailable()
    {
        return status.available();
    }

    @GET
    @Path( STATUS )
    public Response status()
    {
        return status.description();
    }

    public static Pattern databaseClusterUriPattern( Config config )
    {
        return Pattern.compile( config.get( ServerSettings.db_api_path ).getPath() + "/[^/]*" + CLUSTER_PATH );
    }

    public static String absoluteDatabaseClusterPath( Config config )
    {
        return config.get( ServerSettings.db_api_path ).getPath() + DB_CLUSTER_PATH;
    }

    static String relativeDatabaseClusterPath( String databaseName )
    {
        return databaseName + CLUSTER_PATH;
    }
}
