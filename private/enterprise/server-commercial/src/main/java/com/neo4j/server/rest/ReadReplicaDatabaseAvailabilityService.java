/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.server.rest.causalclustering.CausalClusteringService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;

/**
 * To be deprecated by {@link CausalClusteringService}.
 */
@Path( ReadReplicaDatabaseAvailabilityService.BASE_PATH )
public class ReadReplicaDatabaseAvailabilityService implements AdvertisableService
{
    static final String BASE_PATH = "server/read-replica";
    private static final String IS_AVAILABLE_PATH = "/available";

    private final boolean isReadReplica;

    public ReadReplicaDatabaseAvailabilityService( @Context OutputFormat output, @Context Database database )
    {
        DatabaseInfo databaseInfo = database.getGraph().getDependencyResolver().resolveDependency( DatabaseInfo.class );
        isReadReplica = DatabaseInfo.READ_REPLICA.equals( databaseInfo );
    }

    @GET
    @Path( IS_AVAILABLE_PATH )
    public Response isAvailable()
    {
        if ( isReadReplica )
        {
            return positiveResponse();
        }
        return status( FORBIDDEN ).build();
    }

    private Response positiveResponse()
    {
        return plainTextResponse( OK, "true" );
    }

    private Response plainTextResponse( Response.Status status, String entityBody )
    {
        return status( status ).type( TEXT_PLAIN_TYPE ).entity( entityBody ).build();
    }

    @Override
    public String getName()
    {
        return "read-replica";
    }

    @Override
    public String getServerPath()
    {
        return BASE_PATH;
    }
}
