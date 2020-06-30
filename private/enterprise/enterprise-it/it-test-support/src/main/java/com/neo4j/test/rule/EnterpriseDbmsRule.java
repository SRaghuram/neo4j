/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.rule;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;

import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.test.rule.EmbeddedDbmsRule;
import org.neo4j.test.rule.TestDirectory;

public class EnterpriseDbmsRule extends EmbeddedDbmsRule
{
    public EnterpriseDbmsRule()
    {
        super();
    }

    public EnterpriseDbmsRule( TestDirectory testDirectory )
    {
        super( testDirectory );
    }

    @Override
    protected DatabaseManagementServiceBuilder newFactory()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() );
    }

    @Override
    public EnterpriseDbmsRule startLazily()
    {
        return (EnterpriseDbmsRule) super.startLazily();
    }
}
