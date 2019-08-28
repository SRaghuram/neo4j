/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import java.net.URI;
import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.DatabaseService;

import static com.neo4j.server.rest.causalclustering.CausalClusteringService.AVAILABLE;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.READ_ONLY;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.STATUS;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.WRITABLE;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.relativeDatabaseClusterPath;
import static com.neo4j.server.rest.causalclustering.LegacyCausalClusteringRedirectService.CC_PATH;

@Path( CC_PATH )
public class LegacyCausalClusteringRedirectService
{
    static final String CC_PATH = "/server/causalclustering";
    private final Config config;
    private final DatabaseService databaseService;

    public LegacyCausalClusteringRedirectService( @Context DatabaseService databaseService, @Context Config config )
    {
        this.config = config;
        this.databaseService = databaseService;
    }

    @GET
    public Response discover()
    {
        var newLocation = ccPathForDefaultDatabase();
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @GET
    @Path( WRITABLE )
    public Response isWritable()
    {
        var newLocation = String.format( "%s/%s", ccPathForDefaultDatabase(), WRITABLE );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @GET
    @Path( READ_ONLY )
    public Response isReadOnly()
    {
        var newLocation = String.format( "%s/%s", ccPathForDefaultDatabase(), READ_ONLY );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @GET
    @Path( AVAILABLE )
    public Response isAvailable()
    {
        var newLocation = String.format( "%s/%s", ccPathForDefaultDatabase(), AVAILABLE );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    @GET
    @Path( STATUS )
    public Response status()
    {
        var newLocation = String.format( "%s/%s", ccPathForDefaultDatabase(), STATUS );
        return Response.temporaryRedirect( URI.create( newLocation ) ).build();
    }

    public static Pattern databaseLegacyClusterUriPattern( Config config )
    {
        return Pattern.compile( config.get( ServerSettings.management_api_path ).getPath() + CC_PATH );
    }

    private String ccPathForDefaultDatabase()
    {
        var defaultDatabaseName = databaseService.getDatabase().databaseName();
        return String.format( "%s/%s", config.get( ServerSettings.db_api_path ).getPath(), relativeDatabaseClusterPath( defaultDatabaseName ) );
    }
}
