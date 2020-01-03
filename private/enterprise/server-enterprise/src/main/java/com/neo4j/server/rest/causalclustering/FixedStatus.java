/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

class FixedStatus implements CausalClusteringStatus
{
    private final Response.Status status;

    FixedStatus( Response.Status status )
    {
        this.status = status;
    }

    @Override
    public Response discover()
    {
        return buildResponse();
    }

    @Override
    public Response available()
    {
        return buildResponse();
    }

    @Override
    public Response readonly()
    {
        return buildResponse();
    }

    @Override
    public Response writable()
    {
        return buildResponse();
    }

    @Override
    public Response description()
    {
        return buildResponse();
    }

    private Response buildResponse()
    {
        return Response.status( status ).build();
    }
}
