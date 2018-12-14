/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit;

import com.neo4j.harness.EnterpriseTestServerBuilders;

import java.io.File;

import org.neo4j.harness.junit.Neo4jRule;

public class EnterpriseNeo4jRule extends Neo4jRule
{
    public EnterpriseNeo4jRule()
    {
        super( EnterpriseTestServerBuilders.newInProcessBuilder() );
    }

    public EnterpriseNeo4jRule( File workingDirectory )
    {
        super( EnterpriseTestServerBuilders.newInProcessBuilder( workingDirectory ) );
    }
}
