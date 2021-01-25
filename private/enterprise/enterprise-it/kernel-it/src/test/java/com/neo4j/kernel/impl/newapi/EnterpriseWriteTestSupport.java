/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import java.nio.file.Path;

import org.neo4j.kernel.impl.newapi.WriteTestSupport;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class EnterpriseWriteTestSupport extends WriteTestSupport
{
    @Override
    protected TestDatabaseManagementServiceBuilder newManagementServiceBuilder( Path storeDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( storeDir ).impermanent();
    }
}
