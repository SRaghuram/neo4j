/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static co.unruly.matchers.OptionalMatchers.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;

@EnterpriseDbmsExtension
class StandaloneDatabaseIdRepositoryInvalidateIT
{
    @Inject
    private DatabaseManagementService dbms;

    @Inject
    private GraphDatabaseAPI api;
    @Inject
    private DatabaseManager databaseManager;

    @Test
    void shouldInvalidateDroppedDatabaseId()
    {
        var databaseIdRepository = databaseManager.databaseIdRepository();
        var databaseName = "woot";

        assertThat( databaseIdRepository.getByName( databaseName ), empty() );

        dbms.createDatabase( databaseName );

        assertThat( databaseIdRepository.getByName( databaseName ), not( empty() ) );

        dbms.dropDatabase( databaseName );

        assertThat( databaseIdRepository.getByName( databaseName ), empty() );
    }
}
