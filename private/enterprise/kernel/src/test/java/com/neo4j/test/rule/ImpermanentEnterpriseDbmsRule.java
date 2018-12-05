/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.rule;

import com.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.rule.ImpermanentDbmsRule;

public class ImpermanentEnterpriseDbmsRule extends ImpermanentDbmsRule
{
    @Override
    protected GraphDatabaseFactory newFactory()
    {
        return maybeSetInternalLogProvider( maybeSetUserLogProvider( new TestEnterpriseGraphDatabaseFactory() ) );
    }
}
