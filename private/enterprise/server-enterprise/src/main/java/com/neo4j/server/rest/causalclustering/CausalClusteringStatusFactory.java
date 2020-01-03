/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.OutputFormat;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

public class CausalClusteringStatusFactory
{
    public static CausalClusteringStatus build( OutputFormat output, DatabaseManagementService managementService, String databaseName,
            ClusterService clusterService )
    {
        var db = findDb( managementService, databaseName );
        if ( db == null )
        {
            return new FixedStatus( NOT_FOUND );
        }

        switch ( db.databaseInfo() )
        {
        case CORE:
            return new CoreStatus( output, db, clusterService );
        case READ_REPLICA:
            return new ReadReplicaStatus( output, db, clusterService );
        default:
            return new FixedStatus( FORBIDDEN );
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
