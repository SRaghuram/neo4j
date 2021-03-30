/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import java.nio.file.Path;

import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class ReadWithSecurityTest extends ReadWithSecurityTestBase<EnterpriseReadTestSupport>
{
    @Override
    public EnterpriseReadTestSupport newTestSupport()
    {
        return new EnterpriseReadTestSupport()
        {
            @Override
            protected TestDatabaseManagementServiceBuilder newManagementServiceBuilder( Path storeDir )
            {
                TestDatabaseManagementServiceBuilder builder = super.newManagementServiceBuilder( storeDir );
                builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_property_indexes, true );
                return builder;
            }
        };
    }
}
