/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest;

import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.causalclustering.core.consensus.roles.RoleProvider;
import com.neo4j.server.rest.causalclustering.CausalClusteringService;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;

/**
 * To be deprecated by {@link CausalClusteringService}.
 */
@Path( CoreDatabaseAvailabilityService.BASE_PATH )
public class CoreDatabaseAvailabilityService implements AdvertisableService
{
    private static final RoleProvider EMPTY_PROVIDER = () -> null;
    public static final String BASE_PATH = "server/core";
    public static final String IS_WRITABLE_PATH = "/writable";
    public static final String IS_AVAILABLE_PATH = "/available";
    public static final String IS_READ_ONLY_PATH = "/read-only";

    private final OutputFormat output;
    private final boolean notCoreDatabaseType;
    private final RoleProvider roleProvider;

    public CoreDatabaseAvailabilityService( @Context OutputFormat output, @Context GraphDatabaseService db )
    {
        this.output = output;
        if ( db instanceof CoreGraphDatabase )
        {
            this.roleProvider = ((CoreGraphDatabase) db).getDependencyResolver().resolveDependency( RoleProvider.class );
            this.notCoreDatabaseType = false;
        }
        else
        {
            this.roleProvider = EMPTY_PROVIDER;
            this.notCoreDatabaseType = true;
        }
    }

    @GET
    public Response discover()
    {
        if ( notCoreDatabaseType )
        {
            return status( FORBIDDEN ).build();
        }

        return output.ok( new CoreDatabaseAvailabilityDiscoveryRepresentation( BASE_PATH, IS_WRITABLE_PATH ) );
    }

    @GET
    @Path( IS_WRITABLE_PATH )
    public Response isWritable()
    {
        if ( notCoreDatabaseType )
        {
            return status( FORBIDDEN ).build();
        }

        if ( roleProvider.currentRole() == Role.LEADER )
        {
            return positiveResponse();
        }

        return negativeResponse();
    }

    @GET
    @Path( IS_READ_ONLY_PATH )
    public Response isReadOnly()
    {
        if ( notCoreDatabaseType )
        {
            return status( FORBIDDEN ).build();
        }

        Role currentRole = roleProvider.currentRole();
        if ( currentRole == Role.FOLLOWER || currentRole == Role.CANDIDATE )
        {
            return positiveResponse();
        }

        return negativeResponse();
    }

    @GET
    @Path( IS_AVAILABLE_PATH )
    public Response isAvailable()
    {
        if ( notCoreDatabaseType )
        {
            return status( FORBIDDEN ).build();
        }

        return positiveResponse();
    }

    @Override
    public String getName()
    {
        return "core";
    }

    @Override
    public String getServerPath()
    {
        return BASE_PATH;
    }

    private static Response negativeResponse()
    {
        return plainTextResponse( NOT_FOUND, "false" );
    }

    private static Response positiveResponse()
    {
        return plainTextResponse( OK, "true" );
    }

    private static Response plainTextResponse( Response.Status status, String entityBody )
    {
        return status( status ).type( TEXT_PLAIN_TYPE ).entity( entityBody ).build();
    }
}
