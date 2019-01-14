/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.rule;

import com.neo4j.test.TestCommercialGraphDatabaseFactory;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.rule.EmbeddedDatabaseRule;
import org.neo4j.test.rule.TestDirectory;

public class CommercialDatabaseRule extends EmbeddedDatabaseRule
{
    public CommercialDatabaseRule( TestDirectory testDirectory )
    {
        super( testDirectory );
    }

    @Override
    protected GraphDatabaseFactory newFactory()
    {
        return new TestCommercialGraphDatabaseFactory();
    }

    @Override
    public CommercialDatabaseRule startLazily()
    {
        return (CommercialDatabaseRule) super.startLazily();
    }
}
