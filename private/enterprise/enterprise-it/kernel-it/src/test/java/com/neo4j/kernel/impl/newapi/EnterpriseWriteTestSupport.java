/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.newapi.WriteTestSupport;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class EnterpriseWriteTestSupport extends WriteTestSupport
{
    @Override
    protected GraphDatabaseService newDb( File storeDir )
    {
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( storeDir ).impermanent().build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }
}
