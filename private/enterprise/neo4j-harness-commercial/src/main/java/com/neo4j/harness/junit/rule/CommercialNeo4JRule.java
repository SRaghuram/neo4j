/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.junit.rule;

import com.neo4j.harness.internal.CommercialTestNeo4jBuilders;

import java.io.File;

import org.neo4j.harness.junit.rule.Neo4jRule;

public class CommercialNeo4JRule extends Neo4jRule
{
    public CommercialNeo4JRule()
    {
        super( CommercialTestNeo4jBuilders.newInProcessBuilder() );
    }

    public CommercialNeo4JRule( File workingDirectory )
    {
        super( CommercialTestNeo4jBuilders.newInProcessBuilder( workingDirectory ) );
    }
}
