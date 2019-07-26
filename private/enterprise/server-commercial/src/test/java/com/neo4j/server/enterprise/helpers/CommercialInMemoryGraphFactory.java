/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise.helpers;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;

import java.io.File;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.server.database.GraphFactory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;

public class CommercialInMemoryGraphFactory implements GraphFactory
{
    @Override
    public DatabaseManagementService newDatabaseManagementService( Config config, ExternalDependencies dependencies )
    {
        Dependencies deps = new Dependencies();
        deps.satisfyDependencies( SystemGraphInitializer.NO_OP );   // disable system graph construction because it will interfere with some tests
        File storeDir = new File( config.get( GraphDatabaseSettings.databases_root_path ).toFile(), DEFAULT_DATABASE_NAME );

        return new TestCommercialDatabaseManagementServiceBuilder( storeDir )
                .setExtensions( dependencies.extensions() )
                .setMonitors( dependencies.monitors() )
                .setExternalDependencies( deps )
                .impermanent()
                .setConfig( BoltConnector.listen_address, "localhost:0" )
                .setConfig( BoltConnector.enabled, TRUE )
                .setConfig( config )
                .build();
    }
}
