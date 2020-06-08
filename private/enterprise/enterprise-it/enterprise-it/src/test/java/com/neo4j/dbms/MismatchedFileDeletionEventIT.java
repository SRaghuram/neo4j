/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.logging.LogAssertions.assertThat;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
class MismatchedFileDeletionEventIT
{
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private TestDirectory testDirectory;

    private final AssertableLogProvider logProvider = new AssertableLogProvider( true );
    private int next = 10;
    private int last = 10;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setInternalLogProvider( logProvider );
    }

    @Test
    void shouldNotHaveAMismatchedEvent()
    {
        createNext();
        for ( int i = 0; i < 4; i++ )
        {
            createNext();
            dropLast();
        }
        dropLast();

        assertThat( logProvider ).doesNotContainMessage( "database was deleted while it was running" );
    }

    private void createNext()
    {
        var systemDatabase = managementService.database( "system" );
        systemDatabase.executeTransactionally( "CREATE DATABASE db" + ++next );
        showDatabases();
    }

    private void dropLast()
    {
        var systemDatabase = managementService.database( "system" );
        systemDatabase.executeTransactionally( "DROP DATABASE db" + ++last );
        showDatabases();
    }

    private void showDatabases()
    {
        var list = ShowDatabasesHelpers.showDatabases( managementService );
        list.forEach( row ->
        {
            assertEquals( row.currentStatus(), "online" );
        } );
    }
}

