/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@ClusterExtension
@TestDirectoryExtension
class CustomSecurityInitializationIT
{
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private TestDirectory directory;

    private DatabaseManagementService dbms;
    private Cluster cluster;

    @BeforeEach
    void setup()
    {
        FileUtils.deleteFile( directory.homeDir().toPath().resolve( "scripts" ).resolve( "initFile" ).toFile() );
    }

    @AfterEach
    void teardown()
    {
        if ( dbms != null )
        {
            dbms.shutdown();
            dbms = null;
        }
        if ( cluster != null )
        {
            cluster.shutdown();
            cluster = null;
        }
    }

    @Test
    void shouldDoCustomInitializationStandalone() throws IOException
    {
        Path initFile = directory.homeDir().toPath().resolve( "scripts" ).resolve( "initFile" );
        writeTestInitializationFile( initFile, "CREATE ROLE testRole" );
        // Given a standalone db and initialization file
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                .setConfig( GraphDatabaseSettings.system_init_file, Path.of( "initFile" ) )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "SHOW ROLES" );
            assertTrue( result.stream().anyMatch( row -> row.get( "role" ).equals( "testRole" ) ) );
            result.close();
        }
    }

    @Test
    void shouldLogInitializationStandalone() throws IOException
    {
        Path initFile = directory.homeDir().toPath().resolve( "scripts" ).resolve( "initFile" );
        writeTestInitializationFile( initFile, "CREATE ROLE testRole" );
        // Given a standalone db and initialization file
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                .setConfig( GraphDatabaseSettings.system_init_file, Path.of( "initFile" ) )
                .setConfig( GraphDatabaseSettings.log_queries, GraphDatabaseSettings.LogQueryLevel.VERBOSE )
                .build();

        dbms.database( SYSTEM_DATABASE_NAME );
        dbms.shutdown();

        Path logsDir = directory.homeDir().toPath().resolve( "logs" );
        var neo4jLog = logsDir.resolve( "security.log" );
        var lines = Files.lines( neo4jLog ).collect( Collectors.toList() );
        assertThat( lines, hasItem( containsString( "Executing security initialization command: CREATE ROLE testRole" ) ) );
    }

    @Test
    void shouldNotDoCustomInitializationWithoutSettingStandalone() throws IOException
    {
        Path initFile = directory.homeDir().toPath().resolve( "scripts" ).resolve( "initFile" );
        writeTestInitializationFile( initFile, "CREATE ROLE testRole" );
        // Given a standalone db and initialization file
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "SHOW ROLES" );
            assertFalse( result.stream().anyMatch( row -> row.get( "role" ).equals( "testRole" ) ) );
            result.close();
        }
    }

    @Test
    void shouldNotDoCustomInitializationOnSecondStartupStandalone() throws IOException
    {
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                .build();
        dbms.database( SYSTEM_DATABASE_NAME );
        dbms.shutdown();

        Path initFile = directory.homeDir().toPath().resolve( "scripts" ).resolve( "initFile" );
        writeTestInitializationFile( initFile, "CREATE ROLE testRole" );
        // Given a standalone db and initialization file
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                .setConfig( GraphDatabaseSettings.system_init_file, Path.of( "initFile" ) )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "SHOW ROLES" );
            assertFalse( result.stream().anyMatch( row -> row.get( "role" ).equals( "testRole" ) ) );
            result.close();
        }
    }

    @Test
    void shouldFailOnMissingCustomInitializationStandalone()
    {
        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                        .impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                        .setConfig( GraphDatabaseSettings.system_init_file, Path.of( "initFile" ) );
        Exception exception = assertThrows( Exception.class, () -> dbms = builder.build() );

        assertTrue( isFileNotFoundException( exception ) );
    }

    @Test
    void shouldFailOnComplexCustomInitializationWithSyntaxErrorStandalone() throws IOException
    {
        Path initFile = directory.homeDir().toPath().resolve( "scripts" ).resolve( "initFile" );
        writeComplexInitialization( initFile, "(name, email)", "User, Person" );
        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                        .impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                        .setConfig( GraphDatabaseSettings.system_init_file, Path.of( "initFile" ) );
        Exception exception = assertThrows( Exception.class, () -> dbms = builder.build() );
        assertThat( exception.getCause().getMessage(), containsString( "Invalid input '('" ) );
    }

    @Test
    void shouldDoMoreComplexCustomInitializationStandalone() throws IOException
    {
        Path initFile = directory.homeDir().toPath().resolve( "scripts" ).resolve( "initFile" );
        writeComplexInitialization( initFile, "{name, email}", "User, Person" );
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                .setConfig( GraphDatabaseSettings.system_init_file, Path.of( "initFile" ) )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            var result = new TestResultVisitor();
            tx.execute( "SHOW ROLE testRole PRIVILEGES" ).accept( result );
            assertThat( "Should get specific number of privileges for testRole", result.results.size(), equalTo( 7 ) );
        }
    }

    @Test
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldDoCustomInitializationClustered() throws Exception
    {
        // Given a standalone db and initialization file
        var clusterConfig = ClusterConfig.clusterConfig()
                                         .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                                         .withSharedCoreParam( GraphDatabaseSettings.system_init_file, "initFile" )
                                         .withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        for ( ClusterMember member : cluster.coreMembers() )
        {
            File home = member.databaseLayout().getNeo4jLayout().homeDirectory();
            org.apache.commons.io.FileUtils.forceMkdir( home );
            Path initFile = home.toPath().resolve( "scripts" ).resolve( "initFile" );
            writeTestInitializationFile( initFile, "CREATE ROLE testRole" );
        }
        cluster.start();
        cluster.systemTx( ( db, tx ) -> {
            Result result = tx.execute( "SHOW ROLES" );
            assertTrue( result.stream().anyMatch( row -> row.get( "role" ).equals( "testRole" ) ), "Expect to find new role" );
            result.close();
        } );
    }

    @Test
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldLogInitializationClustered() throws Exception
    {
        // Given a cluster and initialization file
        var clusterConfig = ClusterConfig.clusterConfig()
                                         .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                                         .withSharedCoreParam( GraphDatabaseSettings.system_init_file, "initFile" )
                                         .withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        for ( ClusterMember member : cluster.coreMembers() )
        {
            File home = member.databaseLayout().getNeo4jLayout().homeDirectory();
            org.apache.commons.io.FileUtils.forceMkdir( home );
            Path initFile = home.toPath().resolve( "scripts" ).resolve( "initFile" );
            writeTestInitializationFile( initFile, "CREATE ROLE testRole" );
        }
        cluster.start();
        var leader = cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        var logsDir = leader.homeDir().toPath().resolve( "logs" );
        cluster.shutdown();

        var neo4jLog = logsDir.resolve( "security.log" );
        var lines = Files.lines( neo4jLog ).collect( Collectors.toList() );
        assertThat( lines, hasItem( containsString( "Executing security initialization command: CREATE ROLE testRole" ) ) );
    }

    @Disabled
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldFailOnMissingCustomInitializationClustered()
    {
        var clusterConfig = ClusterConfig.clusterConfig()
                                         .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                                         .withSharedCoreParam( GraphDatabaseSettings.system_init_file, "initFile" )
                                         .withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        Exception exception = assertThrows( Exception.class, () -> cluster.start() );
        assertTrue( isFileNotFoundException( exception ) );
    }

    private boolean isFileNotFoundException( Throwable e )
    {
        return e != null && (e instanceof FileNotFoundException || isFileNotFoundException( e.getCause() ));
    }

    private void writeTestInitializationFile( Path initFile, String... lines ) throws IOException
    {
        initFile.getParent().toFile().mkdirs();
        File file = initFile.toFile();
        BufferedWriter writer = new BufferedWriter( new FileWriter( file ) );
        for ( String line : lines )
        {
            writer.write( line + ";" );
            writer.newLine();
        }
        writer.close();
    }

    @SuppressWarnings( "SameParameterValue" )
    private void writeComplexInitialization( Path initFile, String readProperties, String readLabels ) throws IOException
    {
        writeTestInitializationFile( initFile,
                "SHOW ROLES",
                "// Comments should work",
                "", // blank lines should work
                "CREATE ROLE testRole",
                "CREATE DATABASE foo",
                "GRANT ACCESS ON DATABASE foo TO testRole",
                "GRANT TRAVERSE ON GRAPH foo TO testRole",
                "GRANT READ " + readProperties + "\n" +
                        "    ON GRAPH foo\n" +
                        "    NODES " + readLabels + "\n" +
                        "    TO testRole",
                "CREATE USER foo SET PASSWORD 'bar' CHANGE NOT REQUIRED",
                "GRANT ROLE testRole to foo;",  // Extra ';' should work
                "SHOW USER foo PRIVILEGES"
        );
    }

    private static class TestResultVisitor implements Result.ResultVisitor<RuntimeException>
    {
        private List<Result.ResultRow> results = new ArrayList<>();

        @Override
        public boolean visit( Result.ResultRow row )
        {
            results.add( row );
            return true;
        }
    }
}
