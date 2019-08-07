/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import java.util.regex.Pattern;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.server.database.DatabaseService;
import org.neo4j.server.rest.repr.OutputFormat;

@Path( CausalClusteringService.BASE_PATH )
public class CausalClusteringService
{
    public static final Pattern URI_WHITELIST = Pattern.compile( "/db/manage/server/causalclustering.*" );

    static final String BASE_PATH = "server/causalclustering/";

    static final String AVAILABLE = "available";
    static final String WRITABLE = "writable";
    static final String READ_ONLY = "read-only";
    static final String DESCRIPTION = "status";

    private final CausalClusteringStatus status;

    public CausalClusteringService( @Context OutputFormat output, @Context DatabaseService database )
    {
        this.status = CausalClusteringStatusFactory.build( output, database );
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
    @Path( DESCRIPTION )
    public Response status()
    {
        return status.description();
    }
}
