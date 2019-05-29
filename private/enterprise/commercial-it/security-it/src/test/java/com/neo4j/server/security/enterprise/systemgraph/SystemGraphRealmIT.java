/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.IncorrectCredentialsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.cypher.security.BasicSystemGraphRealmIT.SIMULATED_INITIAL_PASSWORD;
import static org.neo4j.cypher.security.BasicSystemGraphRealmIT.simulateSetInitialPasswordCommand;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.testAuthenticationToken;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.UserManager.INITIAL_USER_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

@ExtendWith( TestDirectoryExtension.class )
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

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "alice" ) );
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

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "alice" ) );
        assertThat( realm.getUsernamesForRole( "goon" ), contains( "bob" ) );
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
                .initialUsers( UserManager.INITIAL_USER_NAME )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( UserManager.INITIAL_USER_NAME ) );
        assertAuthenticationSucceeds( realm, UserManager.INITIAL_USER_NAME );
        log.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, UserManager.INITIAL_USER_NAME )
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

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( UserManager.INITIAL_USER_NAME ) );
        assertIncorrectCredentials( realm, UserManager.INITIAL_USER_NAME, UserManager.INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realm, UserManager.INITIAL_USER_NAME,  SIMULATED_INITIAL_PASSWORD  );
    }

    @Test
    void shouldLoadInitialUserWithInitialPassword() throws Throwable
    {
        // Given
        simulateSetInitialPasswordCommand(testDirectory);

        // When
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( dbManager, testDirectory, securityLog );

        // Then
        final User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( dbManager, testDirectory, securityLog );

        User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertTrue( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.passwordChangeRequired() );

        realm.stop();

        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
        assertFalse( user.credentials().matchesPassword( password( INITIAL_PASSWORD ) ) );
        assertTrue( user.credentials().matchesPassword( password( SIMULATED_INITIAL_PASSWORD ) ) );
        assertFalse( user.passwordChangeRequired() );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given started and stopped database
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( dbManager, testDirectory, securityLog );
        realm.setUserPassword( INITIAL_USER_NAME, UTF8.encode( "neo4j2" ), false );
        realm.stop();
        simulateSetInitialPasswordCommand(testDirectory);

        // When
        realm.start();

        // Then
        User user = realm.silentlyGetUser( INITIAL_USER_NAME );
        assertNotNull( user );
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
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "neo4j" ) );
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

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "jane" ) );
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

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "neo4j" ) );
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
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .defaultAdmins( "trinity" )
                .build(), securityLog, dbManager, defaultConfig
        );

        // Then
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "trinity" ) );
        log.assertExactly(
                info( "Completed import of %s %s into system graph.", "3", "users" ),
                info( "Completed import of %s %s into system graph.", "0", "roles" ),
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "trinity" )
        );
    }

    @Test
    void shouldNotAssignAdminWhenExplicitlyImportingRole() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldPerformImport()
                .mayNotPerformMigration()
                .importUsers( "alice" )
                .importRole( "not_admin", "alice" )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertThat( realm.getUsernamesForRole( "not_admin" ), contains( "alice" ) );
        assertTrue( realm.silentlyGetUsernamesForRole( PredefinedRoles.ADMIN ).isEmpty() );
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

        // When
        Set<DatabasePrivilege> privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.READER ) );

        // Then
        DatabasePrivilege expected = new DatabasePrivilege();
        expected.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        expected.addPrivilege( new ResourcePrivilege( Action.FIND, new Resource.GraphResource() ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.EDITOR ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.PUBLISHER ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.TokenResource() ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ARCHITECT ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SchemaResource() ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ADMIN ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SystemResource() ) );
        assertThat( privileges, contains( expected ) );
    }

    @Test
    void shouldSetPrivilegesForCustomRoles() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateRole( PredefinedRoles.ADMIN )
                .migrateRole( "custom" )
                .migrateRole( "role" )
                .build(), securityLog, dbManager, defaultConfig );

        // When
        DatabasePrivilege customPriv = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        customPriv.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        customPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );

        DatabasePrivilege rolePriv = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        rolePriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
        rolePriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.TokenResource() ) );
        rolePriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SchemaResource() ) );

        realm.grantPrivilegeToRole( "custom", customPriv );
        realm.grantPrivilegeToRole( "role", rolePriv );

        // Then
        assertThat( realm.getPrivilegesForRoles( singleton( "custom" ) ), contains( customPriv ) );
        assertThat( realm.getPrivilegesForRoles( singleton( "role" ) ), contains( rolePriv ) );
    }

    @Test
    void shouldSetAdminForCustomRole() throws Throwable
    {
        // TODO
        // If PredefinedRoles.ADMIN does not exist and no users exist, it tries to create neo4j and assign it admin role which fails
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice" )
                .migrateRole( PredefinedRoles.ADMIN )
                .migrateRole( "CustomAdmin" )
                .build(), securityLog, dbManager, defaultConfig );

        // When
        DatabasePrivilege dbPriv = new DatabasePrivilege();
        dbPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SystemResource() ) );
        realm.grantPrivilegeToRole( "CustomAdmin", dbPriv );

        // Then
        assertThat( realm.getPrivilegesForRoles( singleton( "CustomAdmin" ) ), contains( dbPriv ) );

        // When
        realm.revokePrivilegeFromRole( "CustomAdmin", dbPriv );

        // Then
        assertThat( realm.getPrivilegesForRoles( singleton( "CustomAdmin" ) ), empty() );
    }

    @Test
    void shouldFailSetPrivilegesForNonExistingRole() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager, defaultConfig );

        try
        {
            // When
            DatabasePrivilege dbPriv = new DatabasePrivilege();
            dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
            realm.grantPrivilegeToRole( "custom", dbPriv );
            fail( "Should not allow setting privilege on non existing role." );
        }
        catch ( InvalidArgumentsException e )
        {
            // Then
            assertThat( e.getMessage(), equalTo( "Role 'custom' does not exist." ) );
        }
    }

    @Test
    void shouldGetCorrectPrivilegesForMultipleDatabases() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .migrateRole( "custom", "bob" )
                .build(), securityLog, dbManager, defaultConfig );

        DatabasePrivilege dbPriv1 = new DatabasePrivilege( SYSTEM_DATABASE_NAME );
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SchemaResource() ) );

        DatabasePrivilege dbPriv2 = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPriv2.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        dbPriv2.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );

        realm.grantPrivilegeToRole( "custom", dbPriv1 );
        realm.grantPrivilegeToRole( "custom", dbPriv2 );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "bob" );

        assertThat( privileges, containsInAnyOrder( dbPriv1, dbPriv2 ) );
    }

    @Test
    void shouldShowPrivilegesForUser() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .migrateRole( "custom", "bob" )
                .build(), securityLog, dbManager, defaultConfig );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "bob" );
        assertTrue( privileges.isEmpty() );

        DatabasePrivilege dbPriv = new DatabasePrivilege();
        dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        realm.grantPrivilegeToRole( "custom", dbPriv );
        privileges = realm.showPrivilegesForUser( "bob" );
        DatabasePrivilege databasePrivilege = new DatabasePrivilege();
        databasePrivilege.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        assertThat( privileges, containsInAnyOrder( databasePrivilege ) );
    }

    @Test
    void shouldShowAdminPrivileges() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .migrateRole( "custom", "bob" )
                .build(), securityLog, dbManager, defaultConfig );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "bob" );
        assertTrue( privileges.isEmpty() );

        DatabasePrivilege databasePrivilege = new DatabasePrivilege();
        databasePrivilege.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SystemResource() ) );
        realm.grantPrivilegeToRole( "custom", databasePrivilege );

        // Then
        privileges = realm.showPrivilegesForUser( "bob" );
        assertThat( privileges, containsInAnyOrder( databasePrivilege ) );

        // When
        realm.revokePrivilegeFromRole( "custom", databasePrivilege );

        // Then
        privileges = realm.showPrivilegesForUser( "bob" );
        assertTrue( privileges.isEmpty() );
    }

    @Test
    @Disabled
    void shouldHandleCustomDefaultDatabase() throws Throwable
    {
        dbManager.getManagementService().createDatabase( "foo" );
        defaultConfig.augment( default_database, "foo" );

        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( UserManager.INITIAL_USER_NAME )
                .build(), securityLog, dbManager, defaultConfig
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( UserManager.INITIAL_USER_NAME ) );
        assertAuthenticationSucceeds( realm, UserManager.INITIAL_USER_NAME );
        log.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, UserManager.INITIAL_USER_NAME )
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

        // Give Alice read privileges in 'neo4j'
        DatabasePrivilege dbPriv = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        realm.grantPrivilegeToRole( "custom", dbPriv );

        assertAuthenticationSucceeds( realm, "alice" );
        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "alice" );
        assertThat( privileges, containsInAnyOrder( dbPriv ) );

        realm.stop();

        // Create a new database 'foo' and set it to default db in config
        dbManager.getManagementService().createDatabase( "foo" );
        defaultConfig.augment( default_database, "foo" );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realm, "alice" );

        // Alice should still have read privileges in 'neo4j'
        privileges = realm.showPrivilegesForUser( "alice" );
        assertThat( privileges, containsInAnyOrder( dbPriv ) );

        // Alice should NOT have read privileges in 'foo'
        DatabasePrivilege dbPrivFoo = new DatabasePrivilege( "foo" );
        dbPrivFoo.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        assertFalse( privileges.contains( dbPrivFoo ) );

        realm.stop();

        // Switch back default db to 'neo4j'
        defaultConfig.augment( default_database, DEFAULT_DATABASE_NAME );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realm, "alice" );

        // Alice should still have read privileges in 'neo4j'
        privileges = realm.showPrivilegesForUser( "alice" );
        assertThat( privileges, containsInAnyOrder( dbPriv ) );
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
