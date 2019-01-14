/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import com.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.newapi.WriteTestSupport;

public class EnterpriseWriteTestSupport extends WriteTestSupport
{
    @Override
    protected GraphDatabaseService newDb( File storeDir )
    {
        return new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabaseBuilder( storeDir ).newGraphDatabase();
    }
}
