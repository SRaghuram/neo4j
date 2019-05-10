/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.extension;

import org.junit.jupiter.api.Test;

import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

@CommercialDbmsExtension( configurationCallback = "configureGlobal" )
class CommercialDbmsExtensionConfigurationTest
{
    @Inject
    private DatabaseManagementService dbms;

    @ExtensionCallback
    static void configureGlobal( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( default_database, "global" );
    }

    @Test
    void globalConfig()
    {
        dbms.database( "global" );
    }
}
