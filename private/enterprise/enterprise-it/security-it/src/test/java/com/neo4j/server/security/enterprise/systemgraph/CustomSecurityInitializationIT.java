/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.server.security.enterprise.auth.FileRoleRepository;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;

@ClusterExtension
@TestDirectoryExtension
class CustomSecurityInitializationIT
{
    private static final String AUTH_FILENAME = "auth";
    private static final String ROLES_FILENAME = "roles";
    private static final String INIT_FILENAME = "initFile";
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private TestDirectory directory;

    private DatabaseManagementService dbms;
    private Cluster cluster;
    private final LogProvider logProvider = NullLogProvider.getInstance();

    @BeforeEach
    void setup() throws IOException
    {
        FileUtils.deleteRecursively( directory.homeDir() );
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

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldDoCustomInitializationStandalone( String authEnabled ) throws IOException
    {
        writeTestInitializationFile( getInitFile( directory.homeDir() ), "CREATE ROLE testRole" );
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "SHOW ROLES" );
            List<String> roles = result.stream().map( row -> (String) row.get( "role" ) ).collect( Collectors.toList() );
            assertThat( "Should see both default and custom roles", roles,
                    containsInAnyOrder( "testRole", "admin", "architect", "publisher", "editor", "reader", "PUBLIC" ) );
            result.close();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldDoCustomInitializationStandaloneWithAuthRoleMigration( String authEnabled ) throws IOException
    {
        TreeSet<String> users = new TreeSet<>();
        users.add( "neo4j" );
        writeTestAuthFile( getAuthFile( directory.homeDir() ), new User.Builder( "neo4j", credentialFor( "abc123" ) ).build() );
        writeTestRolesFile( getRoleFile( directory.homeDir() ), new RoleRecord.Builder().withName( "custom" ).withUsers( users ).build() );
        writeTestInitializationFile( getInitFile( directory.homeDir() ), "CREATE ROLE testRole", "GRANT ROLE testRole TO neo4j");
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                //.impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            ArrayList<String> roleUsers = new ArrayList<>();
            Result result = tx.execute( "SHOW POPULATED ROLES WITH USERS" );
            result.stream().forEach( r -> roleUsers.add( r.get( "role" ) + "-" + r.get( "member" ) ) );
            result.close();
            assertThat( roleUsers, containsInAnyOrder( "custom-neo4j", "testRole-neo4j" ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldNotDoAuthMigrationWhenFailingCustomInitializationStandalone( String authEnabled ) throws IOException
    {
        TreeSet<String> users = new TreeSet<>();
        users.add( "neo4j" );
        writeTestAuthFile( getAuthFile( directory.homeDir() ), new User.Builder( "neo4j", credentialFor( "abc123" ) ).build() );
        writeTestRolesFile( getRoleFile( directory.homeDir() ), new RoleRecord.Builder().withName( "custom" ).withUsers( users ).build() );
        writeTestInitializationFile( getInitFile( directory.homeDir() ), "CREATE ROLE testRole", "GRANT ROLE testRole TO neo4j", "INVALID CYPHER" );
        TestEnterpriseDatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) )
                .setConfig( GraphDatabaseSettings.log_queries, GraphDatabaseSettings.LogQueryLevel.VERBOSE );
        assertThrows( Exception.class, () -> dbms = builder.build() );

        // change the role name to be migrated to be show that migration happens now
        writeTestRolesFile( getRoleFile( directory.homeDir() ), new RoleRecord.Builder().withName( "custom2" ).withUsers( users ).build() );
        // Then if we fix the init file
        writeTestInitializationFile( getInitFile( directory.homeDir() ), "CREATE ROLE testRole2", "GRANT ROLE testRole2 TO neo4j", "//INVALID CYPHER" );
        dbms = builder.build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            ArrayList<String> roleUsers = new ArrayList<>();
            Result result = tx.execute( "SHOW POPULATED ROLES WITH USERS" );
            result.stream().forEach( r -> roleUsers.add( r.get( "role" ) + "-" + r.get( "member" ) ) );
            result.close();
            assertThat( roleUsers, containsInAnyOrder( "custom2-neo4j", "testRole2-neo4j" ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldLogInitializationStandalone( String authEnabled ) throws IOException
    {
        writeTestInitializationFile( getInitFile( directory.homeDir() ), "CREATE ROLE testRole" );
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) )
                .setConfig( GraphDatabaseSettings.log_queries, GraphDatabaseSettings.LogQueryLevel.VERBOSE )
                .build();

        dbms.database( SYSTEM_DATABASE_NAME );
        dbms.shutdown();

        Path logsDir = directory.homeDir().toPath().resolve( "logs" );
        var neo4jLog = logsDir.resolve( "security.log" );
        try ( var stringStream = Files.lines( neo4jLog ) )
        {
            var lines = stringStream.collect( Collectors.toList() );
            assertThat( lines, hasItem( containsString( "Executing security initialization command: CREATE ROLE testRole" ) ) );
        }
    }

    @Test
    void shouldNotDoCustomInitializationWithoutSettingStandalone() throws IOException
    {
        writeTestInitializationFile( getInitFile( directory.homeDir() ), "CREATE ROLE testRole" );
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

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldNotDoCustomInitializationOnSecondStartupStandalone( String authEnabled ) throws IOException
    {
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.TRUE )
                .build();
        dbms.database( SYSTEM_DATABASE_NAME );
        dbms.shutdown();

        writeTestInitializationFile( getInitFile( directory.homeDir() ), "CREATE ROLE testRole" );
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            Result result = tx.execute( "SHOW ROLES" );
            assertFalse( result.stream().anyMatch( row -> row.get( "role" ).equals( "testRole" ) ) );
            result.close();
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldFailOnMissingCustomInitializationStandalone( String authEnabled )
    {
        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                        .impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                        .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) );
        Exception exception = assertThrows( Exception.class, () -> dbms = builder.build() );

        assertTrue( isFileNotFoundException( exception ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldFailOnComplexCustomInitializationWithSyntaxErrorStandalone( String authEnabled ) throws IOException
    {
        writeComplexInitialization( getInitFile( directory.homeDir() ), "(name, email)", "User, Person" );
        TestEnterpriseDatabaseManagementServiceBuilder builder =
                new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                        .impermanent()
                        .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                        .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) );
        Exception exception = assertThrows( Exception.class, () -> dbms = builder.build() );
        assertThat( exception.getCause().getMessage(), containsString( "Invalid input '('" ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    void shouldDoMoreComplexCustomInitializationStandalone( String authEnabled ) throws IOException
    {
        writeComplexInitialization( getInitFile( directory.homeDir() ), "{name, email}", "User, Person" );
        dbms = new TestEnterpriseDatabaseManagementServiceBuilder( directory.homeDir() )
                .impermanent()
                .setConfig( GraphDatabaseSettings.auth_enabled, Boolean.valueOf( authEnabled ) )
                .setConfig( GraphDatabaseInternalSettings.system_init_file, Path.of( INIT_FILENAME ) )
                .build();
        GraphDatabaseService db = dbms.database( SYSTEM_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            var result = new TestResultVisitor();
            tx.execute( "SHOW ROLE testRole PRIVILEGES" ).accept( result );
            assertThat( "Should get specific number of privileges for testRole", result.results.size(), equalTo( 7 ) );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"true", "false"} )
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldDoCustomInitializationClustered( String authEnabled ) throws Exception
    {
        var clusterConfig = ClusterConfig.clusterConfig()
                .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, authEnabled )
                .withSharedCoreParam( GraphDatabaseInternalSettings.system_init_file, INIT_FILENAME )
                .withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        for ( ClusterMember member : cluster.coreMembers() )
        {
            File home = member.databaseLayout().getNeo4jLayout().homeDirectory().toFile();
            org.apache.commons.io.FileUtils.forceMkdir( home );
            writeTestInitializationFile( getInitFile( home ), "CREATE ROLE testRole" );
        }
        cluster.start();
        cluster.systemTx( ( db, tx ) -> {
            Result result = tx.execute( "SHOW ROLES" );
            List<String> roles = result.stream().map( row -> (String) row.get( "role" ) ).collect( Collectors.toList() );
            result.close();
            assertThat( "Should see both default and custom roles", roles,
                    containsInAnyOrder( "testRole", "admin", "architect", "publisher", "editor", "reader", "PUBLIC" ) );
        } );
    }

    @Test
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldDoCustomInitializationClusteredWithAuthRoleMigration() throws Exception
    {
        TreeSet<String> users = new TreeSet<>();
        users.add( "neo4j" );
        var clusterConfig = ClusterConfig.clusterConfig()
                .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                .withSharedCoreParam( GraphDatabaseInternalSettings.system_init_file, INIT_FILENAME )
                .withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        for ( ClusterMember member : cluster.coreMembers() )
        {
            File home = member.databaseLayout().getNeo4jLayout().homeDirectory().toFile();
            org.apache.commons.io.FileUtils.forceMkdir( home );
            writeTestAuthFile( getAuthFile( home ), new User.Builder( "neo4j", credentialFor( "abc123" ) ).build() );
            writeTestRolesFile( getRoleFile( home ), new RoleRecord.Builder().withName( "custom" ).withUsers( users ).build() );
            writeTestInitializationFile( getInitFile( home ), "CREATE ROLE testRole", "GRANT ROLE testRole TO neo4j");
        }
        cluster.start();
        cluster.systemTx( ( db, tx ) -> {
            ArrayList<String> roleUsers = new ArrayList<>();
            Result result = tx.execute( "SHOW POPULATED ROLES WITH USERS" );
            result.stream().forEach( r -> roleUsers.add( r.get( "role" ) + "-" + r.get( "member" ) ) );
            result.close();
            assertThat( roleUsers, containsInAnyOrder( "custom-neo4j", "testRole-neo4j" ) );
        } );
    }

    @Disabled
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldFailCustomInitializationClusteredWithAuthRoleMigration() throws Exception
    {
        TreeSet<String> users = new TreeSet<>();
        users.add( "neo4j" );
        var clusterConfig = ClusterConfig.clusterConfig().withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" ).withSharedCoreParam(
                GraphDatabaseInternalSettings.system_init_file, INIT_FILENAME ).withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        for ( ClusterMember member : cluster.coreMembers() )
        {
            File home = member.databaseLayout().getNeo4jLayout().homeDirectory().toFile();
            org.apache.commons.io.FileUtils.forceMkdir( home );
            writeTestAuthFile( getAuthFile( home ), new User.Builder( "neo4j", credentialFor( "abc123" ) ).build() );
            writeTestRolesFile( getRoleFile( home ), new RoleRecord.Builder().withName( "custom" ).withUsers( users ).build() );
            writeTestInitializationFile( getInitFile( home ), "CREATE ROLE testRole", "GRANT ROLE testRole TO neo4j", "INVALID CYPHER" );
        }
        assertThrows( Exception.class, () -> cluster.start() );

        for ( ClusterMember member : cluster.coreMembers() )
        {
            File home = member.databaseLayout().getNeo4jLayout().homeDirectory().toFile();
            // change the role name to be migrated to be show that migration happens now
            writeTestRolesFile( getRoleFile( home ), new RoleRecord.Builder().withName( "custom2" ).withUsers( users ).build() );
            // When fixing the broken init file, things should now work
            writeTestInitializationFile( getInitFile( home ), "CREATE ROLE testRole2", "GRANT ROLE testRole2 TO neo4j" );
        }
        cluster.start();
        cluster.systemTx( ( db, tx ) ->
        {
            ArrayList<String> roleUsers = new ArrayList<>();
            Result result = tx.execute( "SHOW POPULATED ROLES WITH USERS" );
            result.stream().forEach( r -> roleUsers.add( r.get( "role" ) + "-" + r.get( "member" ) ) );
            result.close();
            assertThat( roleUsers, containsInAnyOrder( "custom2-neo4j", "testRole2-neo4j" ) );
        } );
    }

    @Test
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldLogInitializationClustered() throws Exception
    {
        var clusterConfig = ClusterConfig.clusterConfig()
                                         .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                                         .withSharedCoreParam( GraphDatabaseInternalSettings.system_init_file, INIT_FILENAME )
                                         .withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        for ( ClusterMember member : cluster.coreMembers() )
        {
            File home = member.databaseLayout().getNeo4jLayout().homeDirectory().toFile();
            org.apache.commons.io.FileUtils.forceMkdir( home );
            writeTestInitializationFile( getInitFile( home ), "CREATE ROLE testRole" );
        }
        cluster.start();
        var leader = cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        var logsDir = leader.homeDir().toPath().resolve( "logs" );
        cluster.shutdown();

        var neo4jLog = logsDir.resolve( "security.log" );
        try ( var stringStream = Files.lines( neo4jLog ) )
        {
            var lines = stringStream.collect( Collectors.toList() );
            assertThat( lines, hasItem( containsString( "Executing security initialization command: CREATE ROLE testRole" ) ) );
        }
    }

    @Disabled
    @Timeout( value = 10, unit = TimeUnit.MINUTES )
    void shouldFailOnMissingCustomInitializationClustered()
    {
        var clusterConfig = ClusterConfig.clusterConfig()
                                         .withSharedCoreParam( GraphDatabaseSettings.auth_enabled, "true" )
                                         .withSharedCoreParam( GraphDatabaseInternalSettings.system_init_file, INIT_FILENAME )
                                         .withNumberOfCoreMembers( 3 );
        cluster = clusterFactory.createCluster( clusterConfig );
        Exception exception = assertThrows( Exception.class, () -> cluster.start() );
        assertTrue( isFileNotFoundException( exception ) );
    }

    private Path getInitFile( File homeDir )
    {
        return homeDir.toPath().resolve( "scripts" ).resolve( INIT_FILENAME );
    }

    private Path getAuthFile( File homeDir )
    {
        return homeDir.toPath().resolve( "data" ).resolve( "dbms" ).resolve( AUTH_FILENAME );
    }

    private Path getRoleFile( File homeDir )
    {
        return homeDir.toPath().resolve( "data" ).resolve( "dbms" ).resolve( ROLES_FILENAME );
    }

    private boolean isFileNotFoundException( Throwable e )
    {
        return e != null && (e instanceof FileNotFoundException || isFileNotFoundException( e.getCause() ));
    }

    private void safeCreateUser( FileUserRepository userRepository, User user )
    {
        try
        {
            userRepository.create( user );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private void safeCreateRole( FileRoleRepository roleRepository, RoleRecord role )
    {
        try
        {
            roleRepository.create( role );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private void writeTestAuthFile( Path path, User... users )
    {
        FileUserRepository userRepository = new FileUserRepository( directory.getFileSystem(), path.toFile(), logProvider );
        Arrays.stream( users ).forEach( user -> safeCreateUser( userRepository, user ) );
    }

    private void writeTestRolesFile( Path path, RoleRecord... roles )
    {
        FileRoleRepository roleRepository = new FileRoleRepository( directory.getFileSystem(), path.toFile(), logProvider );
        Arrays.stream( roles ).forEach( role -> safeCreateRole( roleRepository, role ) );
    }

    private void writeTestInitializationFile( Path initFile, String... lines ) throws IOException
    {
        //noinspection ResultOfMethodCallIgnored
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
        private final List<Result.ResultRow> results = new ArrayList<>();

        @Override
        public boolean visit( Result.ResultRow row )
        {
            results.add( row );
            return true;
        }
    }
}
