/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.server.configuration.ServerSettings;

import static com.neo4j.server.rest.causalclustering.ClusteringDbmsService.CLUSTER_PATH;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.Response.Status.OK;

@Path( CLUSTER_PATH )
public class ClusteringDbmsService
{
    public static final String KEY = "dbms/cluster";
    static final String CLUSTER_PATH = "/cluster";

    private final DatabaseManagementService managementService;

    public ClusteringDbmsService( @Context DatabaseManagementService managementService )
    {
        this.managementService = managementService;
    }

    @GET
    @Path( "status" )
    public Response status()
    {
        return Response.status( OK )
                .type( APPLICATION_JSON )
                .entity( new ClusteringDbmsStatus( managementService ) )
                .build();
    }

    public static Pattern dbmsClusterUriPattern()
    {
        return Pattern.compile( ServerSettings.DBMS_MOUNT_POINT + CLUSTER_PATH + ".*" );
    }

    public static String absoluteDbmsClusterPath( Config config )
    {
        return config.get( ServerSettings.dbms_api_path ).getPath() + CLUSTER_PATH;
    }
}
