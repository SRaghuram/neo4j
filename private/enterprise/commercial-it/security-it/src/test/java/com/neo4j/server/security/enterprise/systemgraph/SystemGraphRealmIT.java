/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Resource;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import org.apache.shiro.authc.AuthenticationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealmTestHelper.testAuthenticationToken;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@ExtendWith( TestDirectoryExtension.class )
class SystemGraphRealmIT
{
    private SystemGraphRealmTestHelper.TestDatabaseManager dbManager;
    private AssertableLogProvider log;
    private SecurityLog securityLog;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void setUp()
    {
        dbManager = new SystemGraphRealmTestHelper.TestDatabaseManager( testDirectory );
        log = new AssertableLogProvider();
        securityLog = new SecurityLog( log.getLog( getClass() ) );
    }

    @AfterEach
    void tearDown()
    {
        dbManager.testSystemDb.shutdown();
    }

    @Test
    void shouldImportExplicitAdmin() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldPerformImport()
                .mayNotPerformMigration()
                .importUsers( "alice" )
                .importRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "alice" ) );
        assertAuthenticationSucceeds( realm, "alice" );
        log.assertExactly(
                info( "Completed import of %s %s and %s %s into system graph.", "1", "user", "1", "role" )
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
                .build(), securityLog, dbManager
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "alice" ) );
        assertThat( realm.getUsernamesForRole( "goon" ), contains( "bob" ) );
        assertAuthenticationSucceeds( realm, "alice" );
        assertAuthenticationSucceeds( realm, "bob" );
        log.assertExactly(
                info( "Completed import of %s %s and %s %s into system graph.", "2", "users", "2", "roles" )
        );
    }

    @Test
    void shouldSetInitialUserAsAdminWithPredefinedUsername() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( UserManager.INITIAL_USER_NAME )
                .build(), securityLog, dbManager
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( UserManager.INITIAL_USER_NAME ) );
        assertAuthenticationSucceeds( realm, UserManager.INITIAL_USER_NAME );
        log.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, UserManager.INITIAL_USER_NAME )
        );
    }

    // In alignment with InternalFlatFileRealm we prevent this case (the admin tool currently does not allow it anyways)
    @Test
    void shouldNotSetInitialUsersAsAdminWithCustomUsernames() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( "jane", "joe" )
                .build(), securityLog, dbManager
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
                .build(), securityLog, dbManager
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "jane" ) );
        assertAuthenticationSucceeds( realm, "jane" );
        log.assertExactly(
                info( "Completed import of %s %s and %s %s into system graph.", "1", "user", "0", "roles" ),
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
                () -> TestSystemGraphRealm.testRealm( importOptions, securityLog, dbManager ) );
        assertThat( exception.getMessage(), startsWith( "No roles defined, and cannot determine which user should be admin" ) );
    }

    @Test
    void shouldMigrateDefaultAdminWithMultipleExistingUsers() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "jane", "alice", "neo4j" )
                .build(), securityLog, dbManager
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "neo4j" ) );
        assertAuthenticationSucceeds( realm, "jane" );
        log.assertExactly(
                info( "Completed import of %s %s and %s %s into system graph.", "3", "users", "0", "roles" ),
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
                .build(), securityLog, dbManager
        );

        // Then
        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "trinity" ) );
        log.assertExactly(
                info( "Completed import of %s %s and %s %s into system graph.", "3", "users", "0", "roles" ),
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
                .build(), securityLog, dbManager
        );

        assertThat( realm.getUsernamesForRole( "not_admin" ), contains( "alice" ) );
        assertTrue( realm.silentlyGetUsernamesForRole( PredefinedRoles.ADMIN ).isEmpty() );
        log.assertExactly(
                info( "Completed import of %s %s and %s %s into system graph.", "1", "user", "1", "role" )
        );
    }

    @Test
    void shouldGetDefaultPrivilegesForDefaultRoles() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .build(), securityLog, dbManager );

        // When
        Set<DatabasePrivilege> privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.READER ) );

        // Then
        DatabasePrivilege expected = new DatabasePrivilege( "*" );
        expected.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.EDITOR ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.GRAPH ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.PUBLISHER ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.TOKEN ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ARCHITECT ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.SCHEMA ) );
        assertThat( privileges, contains( expected ) );

        // When
        privileges = realm.getPrivilegesForRoles( Collections.singleton( PredefinedRoles.ADMIN ) );

        // Then
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.SYSTEM ) );
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
                .build(), securityLog, dbManager );

        // When
        DatabasePrivilege customPriv = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        customPriv.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        customPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.GRAPH ) );

        DatabasePrivilege rolePriv = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        rolePriv.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.GRAPH ) );
        rolePriv.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.TOKEN ) );
        rolePriv.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.SCHEMA ) );

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
                .build(), securityLog, dbManager );

        // When
        realm.setAdmin( "CustomAdmin", true );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.SYSTEM ) );

        // Then
        assertThat( realm.getPrivilegesForRoles( singleton( "CustomAdmin" ) ), contains( dbPriv ) );

        // When
        realm.setAdmin( "CustomAdmin", false );

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
                .build(), securityLog, dbManager );

        try
        {
            // When
            DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
            dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
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
                .build(), securityLog, dbManager );

        DatabasePrivilege dbPriv1 = new DatabasePrivilege( "*" );
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.GRAPH ) );
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.SCHEMA ) );

        DatabasePrivilege dbPriv2 = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPriv2.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        dbPriv2.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.GRAPH ) );

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
                .build(), securityLog, dbManager );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "bob" );
        assertTrue( privileges.isEmpty() );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        realm.grantPrivilegeToRole( "custom", dbPriv );
        privileges = realm.showPrivilegesForUser( "bob" );
        DatabasePrivilege databasePrivilege = new DatabasePrivilege( "*" );
        databasePrivilege.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
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
                .build(), securityLog, dbManager );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "bob" );
        assertTrue( privileges.isEmpty() );

        realm.setAdmin( "custom", true );
        privileges = realm.showPrivilegesForUser( "bob" );
        DatabasePrivilege databasePrivilege = new DatabasePrivilege( "*" );
        databasePrivilege.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.SYSTEM ) );
        assertThat( privileges, containsInAnyOrder( databasePrivilege ) );

        realm.setAdmin( "custom", false );
        privileges = realm.showPrivilegesForUser( "bob" );
        assertTrue( privileges.isEmpty() );
    }

    private void prePopulateUsers( String... usernames ) throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldPerformImport()
                .importUsers( usernames )
                .build(), securityLog, dbManager
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
}
