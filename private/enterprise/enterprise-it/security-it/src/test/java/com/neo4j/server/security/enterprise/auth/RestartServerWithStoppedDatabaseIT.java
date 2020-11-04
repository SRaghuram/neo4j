/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

@TestDirectoryExtension
public class RestartServerWithStoppedDatabaseIT
{
    @Inject
    public TestDirectory testDirectory;

    private DatabaseManagementServiceBuilder managementServiceBuilder;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setup()
    {
        managementServiceBuilder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() );
        managementService = managementServiceBuilder.build();
    }

    @AfterEach
    void tearDown() throws IOException
    {
        managementService.shutdown();
        testDirectory.cleanup();
    }

    @Test
    public void showTransactionsWithStoppedDatabase()
    {
        // GIVEN
        managementService.createDatabase( "foo" );
        managementService.shutdownDatabase( "foo" );
        managementService.shutdown();

        // WHEN
        managementService = managementServiceBuilder.build();

        // THEN
        assertDatabaseIsStopped( "foo" );
        GraphDatabaseService neodb = managementService.database( "neo4j" );

        assertThat( neodb.<Long>executeTransactionally( "CALL dbms.listTransactions()", Collections.emptyMap(),
                                                        result -> result.stream().count() ) ).isEqualTo( 1L );
    }

    @Test
    public void showQueriesWithStoppedDatabase()
    {
        // GIVEN
        managementService.createDatabase( "foo" );
        managementService.shutdownDatabase( "foo" );
        managementService.shutdown();

        // WHEN
        managementService = managementServiceBuilder.build();

        // THEN
        assertDatabaseIsStopped( "foo" );
        GraphDatabaseService neodb = managementService.database( "neo4j" );

        assertThat( neodb.<Long>executeTransactionally( "CALL dbms.listQueries()", Collections.emptyMap(),
                                                        result -> result.stream().count() ) ).isEqualTo( 1L );
    }

    @Test
    public void killQueriesWithStoppedDatabase()
    {
        // GIVEN
        managementService.createDatabase( "foo" );
        managementService.shutdownDatabase( "foo" );
        managementService.shutdown();

        // WHEN
        managementService = managementServiceBuilder.build();

        // THEN
        assertDatabaseIsStopped( "foo" );
        GraphDatabaseService neodb = managementService.database( "neo4j" );
        assertThat( neodb.<Long>executeTransactionally( "CALL dbms.killQueries(['query-12345'])", Collections.emptyMap(),
                                                        result -> result.stream().count() ) ).isEqualTo( 1L );
    }

    private void assertDatabaseIsStopped( String database )
    {
        GraphDatabaseService systemdb = managementService.database( "system" );

        // THEN
        systemdb.executeTransactionally( "SHOW DATABASES", Collections.emptyMap(), result ->
        {
            var rows = result.stream().collect( Collectors.toList() );
            rows.forEach( row ->
                          {
                              if ( row.get( "name" ).equals( database ) )
                              {
                                  assertThat( row.get( "currentStatus" ) ).isEqualTo( "offline" );
                              }
                              else
                              {
                                  assertThat( row.get( "currentStatus" ) ).isEqualTo( "online" );
                              }
                          } );
            assertThat( rows.size() ).isEqualTo( 3 );
            return rows.size();
        } );
    }
}
