/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test.rule;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

public class EnterpriseDatabaseRule extends EmbeddedDatabaseRule
{
    public EnterpriseDatabaseRule()
    {
        super();
    }

    public EnterpriseDatabaseRule( TestDirectory testDirectory )
    {
        super( testDirectory );
    }

    @Override
    protected GraphDatabaseFactory newFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    @Override
    public EnterpriseDatabaseRule startLazily()
    {
        return (EnterpriseDatabaseRule) super.startLazily();
    }
}
