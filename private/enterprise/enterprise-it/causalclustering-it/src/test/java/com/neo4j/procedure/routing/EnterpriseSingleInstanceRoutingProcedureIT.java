/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.routing;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

class EnterpriseSingleInstanceRoutingProcedureIT extends CommunitySingleInstanceRoutingProcedureIT
{
    @Override
    protected DatabaseManagementServiceBuilder newGraphDatabaseFactory( Path databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }

    @Test
    void shouldCallRoutingProcedureForStoppedDatabase()
    {
        var databaseName = "stopped-database";
        var dbms = startDbms( new SocketAddress( "neo4j.com", 1111 ) );

        dbms.createDatabase( databaseName );
        assertNotNull( dbms.database( databaseName ) );
        dbms.shutdownDatabase( databaseName );

        assertRoutingProceduresFailForStoppedDatabase( databaseName, dbms.database( SYSTEM_DATABASE_NAME ) );
    }
}
