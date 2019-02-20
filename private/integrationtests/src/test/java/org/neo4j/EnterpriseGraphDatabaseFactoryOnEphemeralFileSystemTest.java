/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;

import org.neo4j.test.GraphDatabaseFactoryOnEphemeralFileSystemTest;
import org.neo4j.test.TestGraphDatabaseFactory;

class EnterpriseGraphDatabaseFactoryOnEphemeralFileSystemTest extends GraphDatabaseFactoryOnEphemeralFileSystemTest
{
    @Override
    protected TestGraphDatabaseFactory createGraphDatabaseFactory()
    {
        return new TestCommercialGraphDatabaseFactory();
    }
}
