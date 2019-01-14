/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit;

import com.neo4j.harness.CommercialTestServerBuilders;

import java.io.File;

import org.neo4j.harness.junit.Neo4jRule;

public class CommercialNeo4jRule extends Neo4jRule
{
    public CommercialNeo4jRule()
    {
        super( CommercialTestServerBuilders.newInProcessBuilder() );
    }

    public CommercialNeo4jRule( File workingDirectory )
    {
        super( CommercialTestServerBuilders.newInProcessBuilder( workingDirectory ) );
    }
}
