/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import javax.ws.rs.core.Response;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.OutputFormat;

class ReadReplicaDatabaseEndpoints extends ClusteringDatabaseEndpoints
{
    private final ReadReplicaDatabaseStatusProvider rrStatusProvider;

    ReadReplicaDatabaseEndpoints( OutputFormat output, GraphDatabaseAPI databaseAPI, PerDatabaseService clusterService )
    {
        super( output, databaseAPI, clusterService );

        this.rrStatusProvider = new ReadReplicaDatabaseStatusProvider( databaseAPI );
    }

    @Override
    public Response available()
    {
        return positiveResponse();
    }

    @Override
    public Response readonly()
    {
        return positiveResponse();
    }

    @Override
    public Response writable()
    {
        return negativeResponse();
    }

    @Override
    public Response description()
    {
        return statusResponse( rrStatusProvider.currentStatus() );
    }
}
