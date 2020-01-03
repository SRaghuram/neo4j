/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel;

import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

@EnterpriseDbmsExtension
public class EnterpriseDatabaseStartupControllerIT
{
    @Inject
    private GraphDatabaseAPI databaseAPI;

    @Test
    void enterpriseDatabaseUseCorrectStartupController()
    {
        var startupController = databaseAPI.getDependencyResolver().resolveDependency( DatabaseStartupController.class );
        assertThat( startupController, instanceOf( DatabaseStartAborter.class ) );
    }
}
