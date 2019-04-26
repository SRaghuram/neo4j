/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;

import java.io.File;

import org.neo4j.test.DatabaseManagementServiceBuilderOnEphemeralFileSystemTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class CommercialDatabaseManagementServiceBuilderOnEphemeralFileSystemTest extends DatabaseManagementServiceBuilderOnEphemeralFileSystemTest
{
    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( File databaseRootDir )
    {
        return new TestCommercialDatabaseManagementServiceBuilder( databaseRootDir );
    }
}
