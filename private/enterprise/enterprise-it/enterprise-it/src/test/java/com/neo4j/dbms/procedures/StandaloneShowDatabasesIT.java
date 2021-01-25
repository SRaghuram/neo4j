/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures;

import com.neo4j.dbms.ShowDatabasesHelpers.ShowDatabasesResultRow;
import com.neo4j.kernel.enterprise.api.security.EnterpriseSecurityContext;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static com.neo4j.dbms.ShowDatabasesHelpers.showDatabases;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.equalityCondition;

@TestDirectoryExtension
class StandaloneShowDatabasesIT
{
    @Inject
    FileSystemAbstraction fs;
    @Inject
    TestDirectory testDirectory;

    private static final Set<String> initialDatabases = Set.of( SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME );
    private GraphDatabaseAPI systemDb;
    private DatabaseManagementService dbms;

    @BeforeEach
    void setup()
    {
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).build();
        systemDb = (GraphDatabaseAPI) dbms.database( SYSTEM_DATABASE_NAME );
    }

    @AfterEach
    void teardown()
    {
        dbms.shutdown();
    }

    @Test
    void shouldDisplayErrorsForFailedDatabase() throws IOException
    {
        // given
        assertCorrectInitialShowDatabases();
        var defaultDb = (GraphDatabaseFacade) dbms.database( DEFAULT_DATABASE_NAME );
        var defaultDbLayout = defaultDb.databaseLayout();

        // when
        execute( "STOP DATABASE " + DEFAULT_DATABASE_NAME, systemDb );

        // then
        assertEventually( "SHOW DATABASES should return correct status for stopped database " + DEFAULT_DATABASE_NAME,
                () -> statusForDatabase( dbms, DEFAULT_DATABASE_NAME ), equalityCondition( STOPPED.description() ), 5, TimeUnit.SECONDS );

        // when
        fs.deleteFile( defaultDbLayout.nodeStore() );
        fs.deleteRecursively( defaultDbLayout.getTransactionLogsDirectory() );
        execute( "START DATABASE " + DEFAULT_DATABASE_NAME, systemDb );

        // then
        assertEventually( "SHOW DATABASES should return an error for " + DEFAULT_DATABASE_NAME,
                () -> errorForDatabase( dbms, DEFAULT_DATABASE_NAME ), s -> !s.isEmpty(), 5, TimeUnit.SECONDS );
        assertEventually( "SHOW DATABASES should return correct status for stopped database " + DEFAULT_DATABASE_NAME,
                () -> statusForDatabase( dbms, DEFAULT_DATABASE_NAME ), equalityCondition( STOPPED.description() ), 5, TimeUnit.SECONDS );
    }

    @Test
    void shouldDisplayStatusChanges()
    {
        // given
        assertCorrectInitialShowDatabases();

        // when
        execute( "STOP DATABASE " + DEFAULT_DATABASE_NAME, systemDb );

        // then
        assertEventually( "SHOW DATABASES should return correct status for stopped database " + DEFAULT_DATABASE_NAME,
                () -> statusForDatabase( dbms, DEFAULT_DATABASE_NAME ), equalityCondition( STOPPED.description() ), 5, TimeUnit.SECONDS );
    }

    @Test
    void shouldShowAdditionalDatabase()
    {
        // given
        assertCorrectInitialShowDatabases();
        var additionalDatabase = "foo";

        // when
        execute( "CREATE DATABASE " + additionalDatabase, systemDb );

        // then
        assertThat( showDatabases( dbms ) ).as( "SHOW DATABASES should return an extra row" ).hasSize( initialDatabases.size() + 1 );
        assertThat( getShowDatabaseNames( dbms ) ).as( "SHOW DATABASES should return one row for each database, including " + additionalDatabase ).contains(
                SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME, additionalDatabase );
        assertEventually( "SHOW DATABASES should return started status for database " + additionalDatabase,
                () -> statusForDatabase( dbms, additionalDatabase ), equalityCondition( STARTED.description() ), 5, TimeUnit.SECONDS );
    }

    // Should not show dropped database
    @Test
    void shouldNotShowDroppedDatabase()
    {
        // given
        assertCorrectInitialShowDatabases();

        // when
        execute( "DROP DATABASE " + DEFAULT_DATABASE_NAME, systemDb );

        // then
        assertEventually( "SHOW DATABASES should return a single", () -> showDatabases( dbms ),
                s -> s.size() == 1, 5, TimeUnit.SECONDS );
        assertEventually( "SHOW DATABASES should return one row for system database", () -> getShowDatabaseNames( dbms ),
                items -> items.contains( SYSTEM_DATABASE_NAME ), 5, TimeUnit.SECONDS );
    }

    private void assertCorrectInitialShowDatabases()
    {
        assertThat( showDatabases( dbms ) ).as( "SHOW DATABASES should return as many rows as initial databases" ).hasSize( initialDatabases.size() );
        assertThat( getShowDatabaseNames( dbms ) ).as( "SHOW DATABASES should return one row for each initial database" )
                .contains( SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME );
    }

    private static Set<String> getShowDatabaseNames( DatabaseManagementService dbms )
    {
        return showDatabases( dbms ).stream().map( ShowDatabasesResultRow::name ).collect( Collectors.toSet() );
    }

    private static void execute( String query, GraphDatabaseAPI systemDb )
    {
        try ( var tx = systemDb.beginTransaction( KernelTransaction.Type.EXPLICIT, EnterpriseSecurityContext.AUTH_DISABLED ) )
        {
            tx.execute( query );
            tx.commit();
        }
    }

    private String statusForDatabase( DatabaseManagementService dbms, String databaseName )
    {
        return showDatabases( dbms ).stream()
                .filter( row -> Objects.equals( row.name(), databaseName ) )
                .map( ShowDatabasesResultRow::currentStatus )
                .findFirst()
                .orElse( "" );
    }

    private String errorForDatabase( DatabaseManagementService dbms, String databaseName )
    {
        return showDatabases( dbms ).stream()
                .filter( row -> Objects.equals( row.name(), databaseName ) )
                .map( ShowDatabasesResultRow::error )
                .findFirst()
                .orElse( "" );
    }
}
