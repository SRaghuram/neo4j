/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Resource;
import com.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.string.UTF8;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;
import static org.neo4j.server.security.auth.BasicAuthManagerTest.clearedPasswordWithSameLenghtAs;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

@ExtendWith( TestDirectoryExtension.class )
class SystemGraphRealmIT
{
    private TestDatabaseManager dbManager;
    private AssertableLogProvider log;
    private SecurityLog securityLog;
    private int maxFailedAttempts;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void setUp()
    {
        dbManager = new TestDatabaseManager();
        log = new AssertableLogProvider();
        securityLog = new SecurityLog( log.getLog( getClass() ) );
        maxFailedAttempts = Config.defaults().get( GraphDatabaseSettings.auth_max_failed_attempts );
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
    void shouldRateLimitAuthentication() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager
        );

        // First make sure one of the users will have a cached successful authentication result for variation
        assertAuthenticationSucceeds( realm, "alice" );

        assertAuthenticationFailsWithTooManyAttempts( realm, "alice", maxFailedAttempts + 1 );
        assertAuthenticationFailsWithTooManyAttempts( realm, "bob", maxFailedAttempts + 1 );
    }

    @Test
    void shouldClearPasswordOnNewUser() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager
        );

        byte[] password = password( "jake" );

        // When
        realm.newUser( "jake", password, true );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLenghtAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnNewUserAlreadyExists() throws Throwable
    {
        // Given
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager
        );

        realm.newUser( "jake", password( "jake" ), true );
        byte[] password = password( "abc123" );

        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, () -> realm.newUser( "jake", password, true ) );
        assertThat( exception.getMessage(), equalTo( "The specified user 'jake' already exists." ) );

        // Then
        assertThat( password, equalTo( clearedPasswordWithSameLenghtAs( "abc123" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnSetUserPassword() throws Throwable
    {
        // Given
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager
        );

        realm.newUser( "jake", password( "abc123" ), false );

        byte[] newPassword = password( "jake" );

        // When
        realm.setUserPassword( "jake", newPassword, false );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLenghtAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldClearPasswordOnSetUserPasswordWithInvalidPassword() throws Throwable
    {
        // Given
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .build(), securityLog, dbManager
        );

        realm.newUser( "jake", password( "jake" ), false );
        byte[] newPassword = password( "jake" );

        // When
        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, () -> realm.setUserPassword( "jake", newPassword, false ) );
        assertThat( exception.getMessage(), equalTo( "Old password and new password cannot be the same." ) );

        // Then
        assertThat( newPassword, equalTo( clearedPasswordWithSameLenghtAs( "jake" ) ) );
        assertAuthenticationSucceeds( realm, "jake" );
    }

    @Test
    void shouldGetDefaultPrivilegesForDefaultRoles() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "admin", "architect", "publisher", "editor", "reader" )
                .migrateRole( PredefinedRoles.ADMIN, "admin" )
                .migrateRole( PredefinedRoles.ARCHITECT, "architect" )
                .migrateRole( PredefinedRoles.PUBLISHER, "publisher" )
                .migrateRole( PredefinedRoles.EDITOR, "editor" )
                .migrateRole( PredefinedRoles.READER, "reader" )
                .build(), securityLog, dbManager );

        // When
        AuthorizationInfo info = getAuthSnapshot( realm, "admin" );

        // Then
        assertThat( info.getStringPermissions(), containsInAnyOrder(
                equalTo( "system:*" ),
                equalTo( String.format( "database:%s:write:schema:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:write:token:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:write:graph:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:read:graph:*", DEFAULT_DATABASE_NAME ) ) ) );
        assertThat( info.getStringPermissions().size(), equalTo( 4 ) );

        // When
        info = getAuthSnapshot( realm, "architect" );
        assertThat( info.getStringPermissions(), containsInAnyOrder(
                equalTo( String.format( "database:%s:write:schema:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:write:token:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:write:graph:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:read:graph:*", DEFAULT_DATABASE_NAME ) ) ) );
        assertThat( info.getStringPermissions().size(), equalTo( 3 ) );

        // When
        info = getAuthSnapshot( realm, "publisher" );

        // Then
        assertThat( info.getStringPermissions(), containsInAnyOrder(
                equalTo( String.format( "database:%s:write:token:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:write:graph:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:read:graph:*", DEFAULT_DATABASE_NAME ) ) ) );
        assertThat( info.getStringPermissions().size(), equalTo( 2 ) );

        // When
        info = getAuthSnapshot( realm, "editor" );

        // Then
        assertThat( info.getStringPermissions(), contains(
                equalTo( String.format( "database:%s:write:graph:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:read:graph:*", DEFAULT_DATABASE_NAME ) ) ) );
        assertThat( info.getStringPermissions().size(), equalTo( 1 ) );

        // When
        info = getAuthSnapshot( realm, "reader" );

        // Then
        assertThat( info.getStringPermissions(), contains(
                String.format( "database:%s:read:graph:*", DEFAULT_DATABASE_NAME ) ) );
        assertThat( info.getStringPermissions().size(), equalTo( 1 ) );
    }

    @Test
    void shouldSetPrivilegesForCustomRoles() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob", "circe" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .migrateRole( "custom", "bob" )
                .migrateRole( "role", "circe" )
                .build(), securityLog, dbManager );

        // When
        realm.grantPrivilegeToRole( "custom", new ResourcePrivilege( Action.READ, Resource.GRAPH, ResourcePrivilege.FULL_SCOPE ) );
        realm.grantPrivilegeToRole( "custom", new ResourcePrivilege( Action.WRITE, Resource.GRAPH, ResourcePrivilege.FULL_SCOPE ) );
        realm.grantPrivilegeToRole( "role", new ResourcePrivilege( Action.WRITE, Resource.GRAPH, ResourcePrivilege.FULL_SCOPE ) );
        realm.grantPrivilegeToRole( "role", new ResourcePrivilege( Action.WRITE, Resource.TOKEN, ResourcePrivilege.FULL_SCOPE ) );
        realm.grantPrivilegeToRole( "role", new ResourcePrivilege( Action.WRITE, Resource.SCHEMA, ResourcePrivilege.FULL_SCOPE ) );

        // Then
        AuthorizationInfo info = getAuthSnapshot( realm, "bob" );
        assertThat( info.getStringPermissions().size(), equalTo( 1 ) );
        assertThat( info.getStringPermissions(), contains(
                equalTo( String.format( "database:%s:write:graph:*", DEFAULT_DATABASE_NAME ) ),
                equalTo( String.format( "database:%s:read:graph:*", DEFAULT_DATABASE_NAME ) ) ) );

        info = getAuthSnapshot( realm, "circe" );
        assertThat( info.getStringPermissions().size(), equalTo( 3 ) );
        assertThat( info.getStringPermissions(), containsInAnyOrder(
                String.format( "database:%s:write:graph:*", DEFAULT_DATABASE_NAME ),
                String.format( "database:%s:write:schema:*", DEFAULT_DATABASE_NAME ),
                String.format( "database:%s:write:token:*", DEFAULT_DATABASE_NAME )
        ) );
    }

    @Test
    void shouldSetAdminForCustomRole() throws Throwable
    {
        SystemGraphRealm realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .migrateRole( "CustomAdmin", "bob" )
                .build(), securityLog, dbManager );

        // When
        realm.setAdmin( "CustomAdmin", true );

        // Then
        AuthorizationInfo info = getAuthSnapshot( realm, "bob" );
        assertThat( info.getStringPermissions(), contains( "system:*" ) );
        assertThat( info.getStringPermissions().size(), equalTo( 1 ) );
        assertThat( info.getRoles(), contains( "CustomAdmin" ) );
        assertThat( info.getRoles().size(), equalTo( 1 ) );

        // When
        realm.setAdmin( "CustomAdmin", false );

        // Then
        info = getAuthSnapshot( realm, "bob" );
        assertThat( info.getStringPermissions().size(), equalTo( 0 ) );
        assertThat( info.getRoles(), contains( "CustomAdmin" ) );
        assertThat( info.getRoles().size(), equalTo( 1 ) );
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
            realm.grantPrivilegeToRole( "custom", new ResourcePrivilege( Action.READ, Resource.GRAPH, ResourcePrivilege.FULL_SCOPE ) );
            fail( "Should not allow setting privilege on non existing role." );
        }
        catch ( InvalidArgumentsException e )
        {
            // Then
            assertThat( e.getMessage(), equalTo( "Role 'custom' does not exist." ) );
        }
    }

    private AuthorizationInfo getAuthSnapshot( SystemGraphRealm realm, String username )
    {
        return realm.getAuthorizationInfoSnapshot( new SimplePrincipalCollection( username, SecuritySettings.SYSTEM_GRAPH_REALM_NAME ) );
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

    private class TestDatabaseManager extends LifecycleAdapter implements DatabaseManager<StandaloneDatabaseContext>
    {
        GraphDatabaseFacade testSystemDb;

        TestDatabaseManager()
        {
            testSystemDb = (GraphDatabaseFacade) new TestCommercialGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder( testDirectory.databaseDir() )
                    .setConfig( GraphDatabaseSettings.auth_enabled, "false" )
                    .newGraphDatabase();
        }

        @Override
        public Optional<StandaloneDatabaseContext> getDatabaseContext( String name )
        {
            if ( SYSTEM_DATABASE_NAME.equals( name ) )
            {
                DependencyResolver dependencyResolver = testSystemDb.getDependencyResolver();
                Database database = dependencyResolver.resolveDependency( Database.class );
                return Optional.of( new StandaloneDatabaseContext( database, testSystemDb ) );
            }
            return Optional.empty();
        }

        @Override
        public StandaloneDatabaseContext createDatabase( String databaseName )
        {
            throw new UnsupportedOperationException( "Call to createDatabase not expected" );
        }

        @Override
        public void dropDatabase( String databaseName )
        {
        }

        @Override
        public void stopDatabase( String databaseName )
        {
        }

        @Override
        public void startDatabase( String databaseName )
        {
        }

        @Override
        public SortedMap<String,StandaloneDatabaseContext> registeredDatabases()
        {
            return Collections.emptySortedMap();
        }
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private static ShiroAuthToken testAuthenticationToken( String username, String password )
    {
        Map<String,Object> authToken = new TreeMap<>();
        authToken.put( AuthToken.PRINCIPAL, username );
        authToken.put( AuthToken.CREDENTIALS, UTF8.encode( password ) );
        return new ShiroAuthToken( authToken );
    }

    private static void assertAuthenticationSucceeds( SystemGraphRealm realm, String username )
    {
        // NOTE: Password is the same as username
        // Try twice to rule out differences if authentication info has been cached or not
        assertNotNull( realm.getAuthenticationInfo( testAuthenticationToken( username, username ) ) );
        assertNotNull( realm.getAuthenticationInfo( testAuthenticationToken( username, username ) ) );

        // Also test the non-cached result explicitly
        assertNotNull( realm.doGetAuthenticationInfo( testAuthenticationToken( username, username ) ) );
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

    private static void assertAuthenticationFailsWithTooManyAttempts( SystemGraphRealm realm, String username, int attempts )
    {
        // NOTE: Password is the same as username
        for ( int i = 0; i < attempts; i++ )
        {
            try
            {
                assertNull( realm.getAuthenticationInfo( testAuthenticationToken( username, "wrong_password" ) ) );
            }
            catch ( ExcessiveAttemptsException e )
            {
                // This is what we were really looking for
                return;
            }
            catch ( AuthenticationException e )
            {
                // This is expected
            }
        }
        fail( "Did not get an ExcessiveAttemptsException after " + attempts + " attempts." );
    }
}
