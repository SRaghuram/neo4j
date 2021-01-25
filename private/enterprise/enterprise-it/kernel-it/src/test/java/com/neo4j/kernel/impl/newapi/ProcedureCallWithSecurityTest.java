/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class ProcedureCallWithSecurityTest extends ProcedureCallWithSecurityTestBase<EnterpriseWriteTestSupport>
{
    @Override
    public EnterpriseWriteTestSupport newTestSupport()
    {
        return new EnterpriseWriteTestSupport()
        {
            @Override
            protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder builder )
            {
                builder = builder.setConfig( GraphDatabaseSettings.auth_enabled, true );
                return super.configure( builder );
            }
        };
    }
}
