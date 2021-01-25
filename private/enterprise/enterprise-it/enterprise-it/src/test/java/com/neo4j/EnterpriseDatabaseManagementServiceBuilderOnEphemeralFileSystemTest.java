/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.nio.file.Path;

import org.neo4j.test.DatabaseManagementServiceBuilderOnEphemeralFileSystemTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class EnterpriseDatabaseManagementServiceBuilderOnEphemeralFileSystemTest extends DatabaseManagementServiceBuilderOnEphemeralFileSystemTest
{
    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( Path databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }
}
