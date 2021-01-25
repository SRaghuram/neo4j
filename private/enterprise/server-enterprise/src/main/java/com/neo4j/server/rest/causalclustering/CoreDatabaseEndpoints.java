/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.core.consensus.roles.Role;

import javax.ws.rs.core.Response;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.OutputFormat;

class CoreDatabaseEndpoints extends ClusteringDatabaseEndpoints
{
    private final CoreDatabaseStatusProvider coreStatusProvider;

    CoreDatabaseEndpoints( OutputFormat output, GraphDatabaseAPI databaseAPI, PerDatabaseService clusterService )
    {
        super( output, databaseAPI, clusterService );

        this.coreStatusProvider = new CoreDatabaseStatusProvider( databaseAPI );
    }

    @Override
    public Response available()
    {
        return positiveResponse();
    }

    @Override
    public Response readonly()
    {
        Role role = coreStatusProvider.currentRole();
        return Role.FOLLOWER == role || Role.CANDIDATE == role ? positiveResponse() : negativeResponse();
    }

    @Override
    public Response writable()
    {
        return coreStatusProvider.currentRole() == Role.LEADER ? positiveResponse() : negativeResponse();
    }

    @Override
    public Response description()
    {
        return statusResponse( coreStatusProvider.currentStatus() );
    }
}
