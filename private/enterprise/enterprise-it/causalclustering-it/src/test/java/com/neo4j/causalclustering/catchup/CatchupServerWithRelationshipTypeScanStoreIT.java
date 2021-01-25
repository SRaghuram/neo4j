/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;

class CatchupServerWithRelationshipTypeScanStoreIT extends CatchupServerIT
{
    @Override
    void configure( TestEnterpriseDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_type_scan_store, true );
        super.configure( builder );
    }
}
