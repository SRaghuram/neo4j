/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabaseSegment;
import com.neo4j.server.security.enterprise.auth.LabelSegment;
import com.neo4j.server.security.enterprise.auth.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.cypher.security.BasicSystemGraphRealmIT.SIMULATED_INITIAL_PASSWORD;
import static org.neo4j.cypher.security.BasicSystemGraphRealmIT.getExistingUser;
import static org.neo4j.cypher.security.BasicSystemGraphRealmIT.simulateSetInitialPasswordCommand;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.testAuthenticationToken;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SCHEMA;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

@TestDirectoryExtension
class SystemGraphRealmIT
{
    private SystemGraphRealmTestHelper.TestDatabaseManager dbManager;
    private AssertableLogProvider log;
    private SecurityLog securityLog;
    private Config defaultConfig;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void setUp()
    {
        dbManager = new SystemGraphRealmTestHelper.TestDatabaseManager( testDirectory );
        log = new AssertableLogProvider();
        securityLog = new SecurityLog( log.getLog( getClass() ) );
        defaultConfig = Config.defaults();
    }

    @AfterEach
    void tearDown()
    {
        dbManager.getManagementService().shutdown();
    }

    @Test
    void shouldImportExplicitAdmin() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldPerformImport()
                .mayNotPerformMigration()
                .importUsers( "alice" )
                .importRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( "alice", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, "alice" );
        log.assertExactly(
                info( "Completed import of %s %s into system graph.", "1", "user" ),
                info( "Completed import of %s %s into system graph.", "1", "role" )
        );
    }

    @Test
    void shouldPerformMigration() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .migrateRole( "goon", "bob" )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( "alice", PredefinedRoles.ADMIN ) );
        assertTrue( dbManager.userHasRole( "bob", "goon" ) );
        assertAuthenticationSucceeds( realm, "alice" );
        assertAuthenticationSucceeds( realm, "bob" );
        log.assertExactly(
                info( "Completed import of %s %s into system graph.", "2", "users" ),
                info( "Completed import of %s %s into system graph.", "2", "roles" )
        );
    }

    @Test
    void shouldSetInitialUserAsAdminWithPredefinedUsername() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( INITIAL_USER_NAME )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME );
        log.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, INITIAL_USER_NAME )
        );
    }

    @Test
    void shouldSetInitialUserAsAdminWithChangedPassword() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUser( "neo4j1", false )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertIncorrectCredentials( realm, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME,  SIMULATED_INITIAL_PASSWORD  );
    }

    @Test
    void shouldLoadInitialUserWithInitialPassword() throws Throwable
    {
        // Given
        simulateSetInitialPasswordCommand(testDirectory);

        // When
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( dbManager, testDirectory, securityLog );

        // Then
        final User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( dbManager, testDirectory, securityLog );

        User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );

        realm.stop();

        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        user = getExistingUser( realm, INITIAL_USER_NAME );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( dbManager, testDirectory, securityLog );
        realm.stop();
        simulateSetInitialPasswordCommand( testDirectory, "neo4j2" );
        realm.start();
        realm.stop();

        // When
        simulateSetInitialPasswordCommand(testDirectory);
        realm.start();

        // Then
        User user = getExistingUser( realm, INITIAL_USER_NAME );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertFalse( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password("neo4j2") ) );
    }

    // In alignment with InternalFlatFileRealm we prevent this case (the admin tool currently does not allow it anyways)
    @Test
    void shouldNotSetInitialUsersAsAdminWithCustomUsernames() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( "jane", "joe" )
                .build(), securityLog, dbManager, defaultConfig
        );

        // Only the default user should have been created instead
        assertTrue( dbManager.userHasRole( "neo4j", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, "neo4j" );
        assertAuthenticationFails( realm, "jane" );
        assertAuthenticationFails( realm, "joe" );
        log.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "neo4j" )
        );
    }

    @Test
    void shouldMigrateOnlyUserAsAdminEvenWithoutRolesFile() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "jane" )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( "jane", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, "jane" );
        log.assertExactly(
                info( "Completed import of %s %s into system graph.", "1", "user" ),
                info( "Completed import of %s %s into system graph.", "0", "roles" ),
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "jane" )
        );
    }

    @Test
    void shouldNotMigrateMultipleExistingUsersAsAdminWithCustomUsernames() throws Throwable
    {
        SystemGraphImportOptions importOptions = new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "jane", "alice" )
                .build();

        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class,
                () -> TestSystemGraphRealm.testRealm( importOptions, securityLog, dbManager, defaultConfig ) );
        assertThat( exception.getMessage(), startsWith( "No roles defined, and cannot determine which user should be admin" ) );
    }

    @Test
    void shouldMigrateDefaultAdminWithMultipleExistingUsers() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "jane", "alice", "neo4j" )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( "neo4j", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, "jane" );
        log.assertExactly(
                info( "Completed import of %s %s into system graph.", "3", "users" ),
                info( "Completed import of %s %s into system graph.", "0", "roles" ),
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "neo4j" )
        );
    }

    @Test
    void shouldSetDefaultAdmin() throws Throwable
    {
        // Given existing users but no admin
        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, () -> prePopulateUsers( "alice", "bob", "trinity" ) );
        assertThat( exception.getMessage(), startsWith( "No roles defined, and cannot determine which user should be admin" ) );

        // When a default admin is set by command
        TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .defaultAdmins( "trinity" )
                .build(), securityLog, dbManager, defaultConfig
        );

        // Then
        assertTrue( dbManager.userHasRole( "trinity", PredefinedRoles.ADMIN ) );
        log.assertExactly(
                info( "Completed import of %s %s into system graph.", "3", "users" ),
                info( "Completed import of %s %s into system graph.", "0", "roles" ),
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "trinity" )
        );
    }

    @Test
    void shouldNotAssignAdminWhenExplicitlyImportingRole() throws Throwable
    {
        TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldPerformImport()
                .mayNotPerformMigration()
                .importUsers( "alice" )
                .importRole( "not_admin", "alice" )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( "alice", "not_admin" ) );
        assertFalse( dbManager.userHasRole( "alice", PredefinedRoles.ADMIN ) );
        assertFalse( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );

        log.assertExactly(
                info( "Completed import of %s %s into system graph.", "1", "user" ),
                info( "Completed import of %s %s into system graph.", "1", "role" )
        );
    }

    @Test
    void shouldGetDefaultPrivilegesForDefaultRoles() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .build(), securityLog, dbManager, defaultConfig );

        ResourcePrivilege accessPrivilege = new ResourcePrivilege( GRANT, ACCESS, new Resource.DatabaseResource(), DatabaseSegment.ALL );
        ResourcePrivilege readNodePrivilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL );
        ResourcePrivilege readRelPrivilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), RelTypeSegment.ALL );
        ResourcePrivilege findNodePrivilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL );
        ResourcePrivilege findRelPrivilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), RelTypeSegment.ALL );
        ResourcePrivilege writeNodePrivilege = new ResourcePrivilege( GRANT, WRITE, new Resource.AllPropertiesResource(), LabelSegment.ALL );
        ResourcePrivilege writeRelPrivilege = new ResourcePrivilege( GRANT, WRITE, new Resource.AllPropertiesResource(), RelTypeSegment.ALL );
        ResourcePrivilege tokenNodePrivilege = new ResourcePrivilege( GRANT, TOKEN, new Resource.DatabaseResource(), DatabaseSegment.ALL );
        ResourcePrivilege schemaNodePrivilege = new ResourcePrivilege( GRANT, SCHEMA, new Resource.DatabaseResource(), DatabaseSegment.ALL );
        ResourcePrivilege adminNodePrivilege = new ResourcePrivilege( GRANT, ADMIN, new Resource.DatabaseResource(), DatabaseSegment.ALL );

        // When
        Set<ResourcePrivilege> privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.READER ) );

        // Then
        assertThat( privileges, containsInAnyOrder( accessPrivilege, readNodePrivilege, readRelPrivilege, findNodePrivilege, findRelPrivilege ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.EDITOR ) );

        // Then
        assertThat( privileges,
                containsInAnyOrder( accessPrivilege, readNodePrivilege, readRelPrivilege, findNodePrivilege, findRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.PUBLISHER ) );

        // Then
        assertThat( privileges,
                containsInAnyOrder( accessPrivilege, readNodePrivilege, readRelPrivilege, findNodePrivilege, findRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege, tokenNodePrivilege ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ARCHITECT ) );

        // Then
        assertThat( privileges,
                containsInAnyOrder( accessPrivilege, readNodePrivilege, readRelPrivilege, findNodePrivilege, findRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege, tokenNodePrivilege, schemaNodePrivilege ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ADMIN ) );

        // Then
        assertThat( privileges,
                containsInAnyOrder( accessPrivilege, readNodePrivilege, readRelPrivilege, findNodePrivilege, findRelPrivilege, writeNodePrivilege,
                        writeRelPrivilege, tokenNodePrivilege, schemaNodePrivilege, adminNodePrivilege ) );
    }

    @Test
    void shouldHandleCustomDefaultDatabase() throws Throwable
    {
        dbManager.getManagementService().createDatabase( "foo" );
        defaultConfig.set( default_database, "foo" );

        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( INITIAL_USER_NAME )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN  ));
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME );
        log.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, INITIAL_USER_NAME )
        );
    }

    @Test
    void shouldHandleSwitchOfDefaultDatabase() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice" )
                .migrateRole( "custom", "alice" )
                .build(), securityLog, dbManager, defaultConfig
        );

        // Give Alice match privileges in 'neo4j'
        ResourcePrivilege readPrivilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        ResourcePrivilege findPrivilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        GraphDatabaseService systemDB = dbManager.getManagementService().database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = systemDB.beginTx() )
        {
            transaction.execute( String.format( "GRANT MATCH {*} ON GRAPH %s NODES * TO %s", DEFAULT_DATABASE_NAME, "custom" ) );
            transaction.commit();
        }

        assertAuthenticationSucceeds( realm, "alice" );
        Set<ResourcePrivilege> privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( readPrivilege, findPrivilege ) );

        realm.stop();

        // Create a new database 'foo' and set it to default db in config
        dbManager.getManagementService().createDatabase( "foo" );
        defaultConfig.set( default_database, "foo" );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realm, "alice" );

        // Alice should still have read privileges in 'neo4j'
        privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( readPrivilege, findPrivilege ) );

        // Alice should NOT have read privileges in 'foo'
        assertFalse( privileges.contains( new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, "foo" ) ) );
        assertFalse( privileges.contains( new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, "foo" ) ) );

        realm.stop();

        // Switch back default db to 'neo4j'
        defaultConfig.set( default_database, DEFAULT_DATABASE_NAME );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realm, "alice" );

        // Alice should still have read privileges in 'neo4j'
        privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( readPrivilege, findPrivilege ) );
    }

    private void prePopulateUsers( String... usernames ) throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldPerformImport()
                .importUsers( usernames )
                .build(), securityLog, dbManager, defaultConfig
        );
        realm.stop();
        realm.shutdown();
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private static void assertAuthenticationFails( SystemGraphRealm realm, String username )
    {
        // NOTE: Password is the same as username
        // Try twice to rule out differences if authentication info has been cached or not
        for ( int i = 0; i < 2; i++ )
        {
            assertThrows( AuthenticationException.class, () -> realm.getAuthenticationInfo( testAuthenticationToken( username, username ) ) );
        }

        // Also test the non-cached result explicitly
        assertThrows( AuthenticationException.class, () -> realm.doGetAuthenticationInfo( testAuthenticationToken( username, username ) ) );
    }

    private static void assertIncorrectCredentials( SystemGraphRealm realm, String username, String password )
    {
        // Try twice to rule out differences if authentication info has been cached or not
        for ( int i = 0; i < 2; i++ )
        {
            assertThrows( IncorrectCredentialsException.class, () -> realm.getAuthenticationInfo( testAuthenticationToken( username, password ) ) );
        }
    }
}
