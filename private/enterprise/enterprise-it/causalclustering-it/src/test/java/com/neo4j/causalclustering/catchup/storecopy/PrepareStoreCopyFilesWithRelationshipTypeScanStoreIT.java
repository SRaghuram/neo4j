/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class PrepareStoreCopyFilesWithRelationshipTypeScanStoreIT extends PrepareStoreCopyFilesIT
{
    @Override
    void doConfigure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
        super.doConfigure( builder );
    }
}
