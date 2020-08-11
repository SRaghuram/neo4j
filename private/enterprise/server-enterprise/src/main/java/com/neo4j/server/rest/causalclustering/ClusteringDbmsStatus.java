/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.OutputStream;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.domain.JsonHelper;

/**
 * A {@link StreamingOutput} implementation that streams a JSON array of objects describing statuses of all available databases.
 * It allows us to avoid building a potentially large JSON string in memory before sending it in an HTTP response.
 */
public class ClusteringDbmsStatus implements StreamingOutput
{
    private static final String NAME_FIELD = "databaseName";
    private static final String UUID_FIELD = "databaseUuid";
    private static final String STATUS_FIELD = "databaseStatus";

    private final DatabaseManagementService managementService;

    public ClusteringDbmsStatus( DatabaseManagementService managementService )
    {
        this.managementService = managementService;
    }

    @Override
    public void write( OutputStream output ) throws IOException, WebApplicationException
    {
        var jsonGenerator = JsonHelper.newJsonGenerator( output );

        jsonGenerator.writeStartArray();
        writeDatabaseStatuses( jsonGenerator );
        jsonGenerator.writeEndArray();

        jsonGenerator.flush();
    }

    private void writeDatabaseStatuses( JsonGenerator jsonGenerator ) throws IOException
    {
        for ( var databaseName : managementService.listDatabases() )
        {
            writeDatabaseStatus( jsonGenerator, databaseName );
        }
    }

    private void writeDatabaseStatus( JsonGenerator jsonGenerator, String databaseName ) throws IOException
    {
        try
        {
            var db = (GraphDatabaseAPI) managementService.database( databaseName );
            if ( !db.isAvailable( 0 ) )
            {
                return;
            }

            jsonGenerator.writeStartObject();
            jsonGenerator.writeObjectField( NAME_FIELD, databaseName );
            jsonGenerator.writeObjectField( UUID_FIELD, db.databaseId().databaseId().uuid() );
            jsonGenerator.writeObjectField( STATUS_FIELD, buildClusterStatus( db ) );
            jsonGenerator.writeEndObject();
        }
        catch ( DatabaseNotFoundException ignore )
        {
            // database no longer exists
        }
    }

    private static ClusteringDatabaseStatusResponse buildClusterStatus( GraphDatabaseAPI db )
    {
        var dbInfo = db.dbmsInfo();
        if ( dbInfo == DbmsInfo.CORE )
        {
            var coreStatus = new CoreDatabaseStatusProvider( db );
            return coreStatus.currentStatus();
        }
        else if ( dbInfo == DbmsInfo.READ_REPLICA )
        {
            var readReplicaStatus = new ReadReplicaDatabaseStatusProvider( db );
            return readReplicaStatus.currentStatus();
        }
        else
        {
            throw new IllegalStateException( "Not possible to get status for " + dbInfo );
        }
    }
}
