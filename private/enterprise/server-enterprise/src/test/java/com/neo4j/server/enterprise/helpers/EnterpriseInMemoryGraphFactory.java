/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.helpers;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.server.database.GraphFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class EnterpriseInMemoryGraphFactory implements GraphFactory
{
    @Override
    public DatabaseManagementService newDatabaseManagementService( Config config, ExternalDependencies dependencies )
    {
        File storeDir = new File( config.get( GraphDatabaseSettings.databases_root_path ).toFile(), DEFAULT_DATABASE_NAME );

        return new TestEnterpriseDatabaseManagementServiceBuilder( storeDir )
                .setExtensions( dependencies.extensions() )
                .setMonitors( dependencies.monitors() )
                .noOpSystemGraphInitializer()
                .impermanent()
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", 0 ) )
                .setConfig( BoltConnector.enabled, true )
                .setConfig( config )
                .build();
    }
}
