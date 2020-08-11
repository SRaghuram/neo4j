/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

class FixedResponse implements ClusteringEndpoints
{
    private Response response;

    FixedResponse( Response response )
    {
        this.response = response;
    }

    FixedResponse( Response.Status status )
    {
        this( Response.status( status ).build() );
    }

    @Override
    public Response discover()
    {
        return response;
    }

    @Override
    public Response available()
    {
        return response;
    }

    @Override
    public Response readonly()
    {
        return response;
    }

    @Override
    public Response writable()
    {
        return response;
    }

    @Override
    public Response description()
    {
        return response;
    }
}
