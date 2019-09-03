/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;

import org.neo4j.server.database.DatabaseService;
import org.neo4j.server.rest.repr.OutputFormat;

public abstract class AbstractCausalClusteringService implements ClusterService
{
    static final String AVAILABLE = "available";
    static final String WRITABLE = "writable";
    static final String READ_ONLY = "read-only";
    static final String STATUS = "status";

    private final CausalClusteringStatus status;

    AbstractCausalClusteringService( OutputFormat output, DatabaseService dbService, String databaseName )
    {
        this.status = CausalClusteringStatusFactory.build( output, dbService, databaseName, this );
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
