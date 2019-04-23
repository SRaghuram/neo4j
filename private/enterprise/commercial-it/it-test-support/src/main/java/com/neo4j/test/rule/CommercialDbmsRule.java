/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.rule;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;

import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.TestDirectory;

public class CommercialDbmsRule extends EmbeddedDbmsRule
{
    public CommercialDbmsRule()
    {
        super();
    }

    public CommercialDbmsRule( TestDirectory testDirectory )
    {
        super( testDirectory );
    }

    @Override
    protected DatabaseManagementServiceBuilder newFactory()
    {
        return new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() );
    }

    @Override
    public CommercialDbmsRule startLazily()
    {
        return (CommercialDbmsRule) super.startLazily();
    }
}
