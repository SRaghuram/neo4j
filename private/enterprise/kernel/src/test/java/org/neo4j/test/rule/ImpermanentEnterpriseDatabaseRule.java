/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.rule;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

public class ImpermanentEnterpriseDatabaseRule extends ImpermanentDatabaseRule
{
    @Override
    protected GraphDatabaseFactory newFactory()
    {
        return maybeSetInternalLogProvider( maybeSetUserLogProvider( new TestEnterpriseGraphDatabaseFactory() ) );
    }
}
