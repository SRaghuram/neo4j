/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.io.File;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.newapi.ReadTestSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class EnterpriseReadTestSupport extends ReadTestSupport
{
    @Override
    protected TestDatabaseManagementServiceBuilder newManagementServiceBuilder( File storeDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.auth_enabled, true )
                .impermanent();
    }
}
