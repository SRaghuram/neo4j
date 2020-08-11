/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.JsonHelper;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.status;

abstract class ClusteringDatabaseEndpoints implements ClusteringEndpoints
{
    protected final OutputFormat output;
    protected final GraphDatabaseAPI db;
    private final PerDatabaseService clusterService;

    ClusteringDatabaseEndpoints( OutputFormat output, GraphDatabaseAPI db, PerDatabaseService clusterService )
    {
        this.output = output;
        this.db = db;
        this.clusterService = clusterService;
    }

    @Override
    public final Response discover()
    {
        return output.ok( new ClusteringEndpointDiscovery( clusterService.relativePath( db.databaseName() ) ) );
    }

    static Response statusResponse( ClusteringDatabaseStatusResponse clusterStatusResponse )
    {
        var jsonObject = JsonHelper.writeValueAsString( clusterStatusResponse );
        return status( OK ).type( MediaType.APPLICATION_JSON ).entity( jsonObject ).build();
    }

    static Response positiveResponse()
    {
        return plainTextResponse( OK, "true" );
    }

    static Response negativeResponse()
    {
        return plainTextResponse( NOT_FOUND, "false" );
    }

    private static Response plainTextResponse( Response.Status status, String entityBody )
    {
        return status( status ).type( TEXT_PLAIN_TYPE ).entity( entityBody ).build();
    }
}
