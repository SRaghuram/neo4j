/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

public abstract class AbstractClusteringDatabaseService implements PerDatabaseService
{
    static final String AVAILABLE = "available";
    static final String WRITABLE = "writable";
    static final String READ_ONLY = "read-only";
    static final String STATUS = "status";

    private final ClusteringEndpoints status;

    AbstractClusteringDatabaseService( OutputFormat output, DatabaseStateService dbStateService, DatabaseManagementService managementService,
                                      String databaseName )
    {
        this.status = createStatus( output, dbStateService, managementService, databaseName );
    }

    private ClusteringEndpoints createStatus( OutputFormat output, DatabaseStateService dbStateService, DatabaseManagementService managementService,
                                             String databaseName )
    {
        try
        {
            return ClusteringDatabaseEndpointsFactory.build( output, dbStateService, managementService, databaseName, this );
        }
        catch ( Exception e )
        {
            return new FixedResponse( Response.status( INTERNAL_SERVER_ERROR ).type( MediaType.TEXT_PLAIN_TYPE ).entity( e.toString() ).build() );
        }
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
}
