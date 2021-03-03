/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */

package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.nio.file.Path;

import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class NodeKeyConstraintCreationSSTIIT extends NodeKeyConstraintCreationIT
{
    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( Path databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir )
                .setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
    }
}
