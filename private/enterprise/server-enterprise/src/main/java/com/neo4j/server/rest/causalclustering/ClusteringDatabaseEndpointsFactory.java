/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.dbms.EnterpriseOperatorState;

import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static javax.ws.rs.core.Response.status;

public class ClusteringDatabaseEndpointsFactory
{
    public static ClusteringEndpoints build( OutputFormat output, DatabaseStateService dbStateService, DatabaseManagementService managementService,
                                            String databaseName, PerDatabaseService clusterService )
    {
        var db = findDb( managementService, databaseName );
        if ( db == null )
        {
            return new FixedResponse( NOT_FOUND );
        }

        // This cast is safe because Causal Clustering endpoints are only available in enterprise mode
        EnterpriseOperatorState operatorState = (EnterpriseOperatorState) dbStateService.stateOfDatabase( db.databaseId() ).operatorState();
        return availabilityAwareStatus( output, databaseName, clusterService, db, operatorState );
    }

    private static ClusteringEndpoints availabilityAwareStatus( OutputFormat output, String databaseName, PerDatabaseService clusterService,
                                                               GraphDatabaseAPI db, EnterpriseOperatorState operatorState )
    {
        switch ( operatorState )
        {
        case INITIAL:
            return new FixedResponse(
                    status( SERVICE_UNAVAILABLE ).header( "Retry-After", 60 ).type( TEXT_PLAIN_TYPE )
                                                 .entity( "Database " + databaseName + " is " + operatorState.description() )
                                                 .build() );
        case STOPPED:
        case DROPPED:
        case DIRTY:
        case UNKNOWN:
            return new FixedResponse( status( SERVICE_UNAVAILABLE ).entity( "Database " + databaseName + " is " + operatorState.description() ).build() );
        default:
            return createStatus( output, clusterService, db );
        }
    }

    private static ClusteringEndpoints createStatus( OutputFormat output, PerDatabaseService clusterService, GraphDatabaseAPI db )
    {
        switch ( db.dbmsInfo() )
        {
        case CORE:
            return new CoreDatabaseEndpoints( output, db, clusterService );
        case READ_REPLICA:
            return new ReadReplicaDatabaseEndpoints( output, db, clusterService );
        default:
            return new FixedResponse( FORBIDDEN );
        }
    }

    private static GraphDatabaseAPI findDb( DatabaseManagementService managementService, String databaseName )
    {
        try
        {
            return (GraphDatabaseAPI) managementService.database( databaseName );
        }
        catch ( DatabaseNotFoundException e )
        {
            return null;
        }
    }
}
