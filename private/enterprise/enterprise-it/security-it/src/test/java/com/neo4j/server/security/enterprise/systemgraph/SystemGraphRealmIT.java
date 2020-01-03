/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.dbms.EnterpriseSystemGraphInitializer;
import com.neo4j.server.security.enterprise.auth.DatabaseSegment;
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.LabelSegment;
import com.neo4j.server.security.enterprise.auth.RelTypeSegment;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.security.SecureHasher;
import org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationFails;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.cypher.security.BasicSystemGraphRealmTestHelper.createUser;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ADMIN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.READ;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.SCHEMA;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRAVERSE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@TestDirectoryExtension
class SystemGraphRealmIT
{
    private TestDatabaseManager dbManager;
    private AssertableLogProvider logProvider;
    private SecurityLog securityLog;
    private Config defaultConfig;
    private SecureHasher secureHasher;

    @Inject
    private TestDirectory testDirectory;

    private InMemoryUserRepository oldUsers;
    private InMemoryRoleRepository oldRoles;
    private InMemoryUserRepository initialPassword;
    private InMemoryUserRepository defaultAdmin;

    @BeforeEach
    void setUp()
    {
        secureHasher = new SecureHasher();
        dbManager = new TestDatabaseManager( testDirectory );
        logProvider = new AssertableLogProvider();
        securityLog = new SecurityLog( logProvider.getLog( getClass() ) );
        defaultConfig = Config.defaults();
        oldUsers = new InMemoryUserRepository();
        oldRoles = new InMemoryRoleRepository();
        initialPassword = new InMemoryUserRepository();
        defaultAdmin = new InMemoryUserRepository();
    }

    @AfterEach
    void tearDown()
    {
        dbManager.getManagementService().shutdown();
    }

    @Test
    void shouldPerformMigration() throws Throwable
    {
        oldUsers.create( createUser( "alice", "foo", false ) );
        oldUsers.create( createUser( "bob", "bar", true ) );

        oldRoles.create( new RoleRecord( PredefinedRoles.ADMIN, "alice" ) );
        oldRoles.create( new RoleRecord( "goon", "bob" ) );

        SystemGraphRealm realm = startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( "alice", PredefinedRoles.ADMIN ) );
        assertTrue( dbManager.userHasRole( "bob", "goon" ) );
        assertAuthenticationSucceeds( realm, "alice", "foo" );
        assertAuthenticationSucceeds( realm, "bob", "bar", true );
        logProvider.assertExactly(
                info( "Completed migration of %s %s into system graph.", "2", "users" ),
                info( "Completed migration of %s %s into system graph.", "2", "roles" )
        );
    }

    @Test
    void shouldCreateDefaultAdminWithPredefinedUsername() throws Throwable
    {
        SystemGraphRealm realm = startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME, INITIAL_PASSWORD, true );
        logProvider.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, INITIAL_USER_NAME )
        );
    }

    @Test
    void shouldSetInitialUserAsAdminWithChangedPassword() throws Throwable
    {
        initialPassword.create( createUser( INITIAL_USER_NAME, "neo4j1", false ) );

        SystemGraphRealm realm = startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertAuthenticationFails( realm, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME, "neo4j1" );
    }

    @Test
    void shouldSetInitialUserWithPasswordChangeRequired() throws Throwable
    {
        initialPassword.create( createUser( INITIAL_USER_NAME, "neo4j1", true ) );

        SystemGraphRealm realm = startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertAuthenticationFails( realm, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME, "neo4j1", true );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        SystemGraphRealm realm = startSystemGraphRealm();
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME, INITIAL_PASSWORD, true );

        realm.stop();

        initialPassword.create( createUser( INITIAL_USER_NAME, "abc123", false ) );

        // When
        realm.start();

        // Then
        assertAuthenticationFails( realm, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME, "abc123" );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given
        SystemGraphRealm realm = startSystemGraphRealm();
        realm.stop();

        initialPassword.create( createUser( INITIAL_USER_NAME, "foo", false ) );

        realm.start();
        realm.stop();

        // When
        initialPassword.clear();
        initialPassword.create( createUser( INITIAL_USER_NAME, "bar", false ) );
        realm.start();

        // Then
        assertAuthenticationFails( realm, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME, "foo" );
        assertAuthenticationFails( realm, INITIAL_USER_NAME, "bar" );
    }

    @Test
    void shouldThrowOnBrokenInitialUsersFile() throws Throwable
    {
        // When
        initialPassword.create( createUser( "jane", "foo", false ) );

        // Then
        IllegalStateException wrongUsernameException = assertThrows( IllegalStateException.class, this::startSystemGraphRealm );
        String wrongUsernameErrorMessage = "Invalid `auth.ini` file: the user in the file is not named " + INITIAL_USER_NAME;
        assertThat( wrongUsernameException.getMessage(), equalTo( wrongUsernameErrorMessage ) );
        logProvider.assertAtLeastOnce( inLog( this.getClass() ).error( containsString( wrongUsernameErrorMessage ) ) );

        // Given
        initialPassword.clear();
        logProvider.clear();

        // When
        initialPassword.create( createUser( "jane", "foo", false ) );
        initialPassword.create( createUser( INITIAL_USER_NAME, "foo", false ) );

        // Then
        IllegalStateException multipleUsersErrorException = assertThrows( IllegalStateException.class, this::startSystemGraphRealm );
        String multipleUsersErrorMessage = "Invalid `auth.ini` file: the file contains more than one user";
        assertThat( multipleUsersErrorException.getMessage(), equalTo(  multipleUsersErrorMessage) );
        logProvider.assertAtLeastOnce( inLog( this.getClass() ).error( containsString( multipleUsersErrorMessage ) ) );
    }

    @Test
    void shouldMigrateOnlyUserAsAdminEvenWithoutRolesFile() throws Throwable
    {
        oldUsers.create( createUser( "jane", "doe", false ) );

        SystemGraphRealm realm = startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( "jane", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, "jane", "doe" );
        logProvider.assertExactly(
                info( "Completed migration of %s %s into system graph.", "1", "user" ),
                info( "Completed migration of %s %s into system graph.", "0", "roles" ),
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "jane" )
        );
    }

    @Test
    void shouldMigrateDefaultAdminWithMultipleExistingUsers() throws Throwable
    {
        oldUsers.create( createUser( "jane", "doe", false ) );
        oldUsers.create( createUser( "alice", "foo", false ) );
        oldUsers.create( createUser( INITIAL_USER_NAME, "bar", false ) );

        SystemGraphRealm realm = startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( "neo4j", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realm, "jane", "doe" );
        logProvider.assertExactly(
                info( "Completed migration of %s %s into system graph.", "3", "users" ),
                info( "Completed migration of %s %s into system graph.", "0", "roles" ),
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "neo4j" )
        );
    }

    @Test
    void shouldNotMigrateMultipleExistingUsersAsAdminWithCustomUsernames() throws Throwable
    {
        oldUsers.create( createUser( "jane", "doe", false ) );
        oldUsers.create( createUser( "alice", "foo", false ) );

        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, this::startSystemGraphRealm );
        assertThat( exception.getMessage(), startsWith( "No roles defined, and cannot determine which user should be admin" ) );
    }

    @Test
    void shouldSetDefaultAdmin() throws Throwable
    {
        oldUsers.create( createUser( "alice", "foo", false ) );
        oldUsers.create( createUser( "bob", "bar", false ) );
        oldUsers.create( createUser( "trinity", "abc", false ) );

        // Given existing users but no admin
        InvalidArgumentsException exception = assertThrows( InvalidArgumentsException.class, this::startSystemGraphRealm );
        assertThat( exception.getMessage(), startsWith( "No roles defined, and cannot determine which user should be admin" ) );

        // When a default admin is set by command
        defaultAdmin.create( createUser( "trinity", "ignored", false ) );

        var realm = startSystemGraphRealm();

        // Then
        assertAuthenticationSucceeds( realm, "trinity", "abc" );
        assertTrue( dbManager.userHasRole( "trinity", PredefinedRoles.ADMIN ) );
        logProvider.assertExactly(
                info( "Completed migration of %s %s into system graph.", "3", "users" ),
                info( "Completed migration of %s %s into system graph.", "0", "roles" ),
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "trinity" )
        );
    }

    @Test
    void shouldGetDefaultPrivilegesForDefaultRoles() throws Throwable
    {
        SystemGraphRealm realm = startSystemGraphRealm();

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

        SystemGraphRealm realm = startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN  ));
        assertAuthenticationSucceeds( realm, INITIAL_USER_NAME, INITIAL_PASSWORD, true );
        logProvider.assertExactly(
                info( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, INITIAL_USER_NAME )
        );
    }

    @Test
    void shouldHandleSwitchOfDefaultDatabase() throws Throwable
    {
        oldUsers.create( createUser( "alice", "bar", false ) );
        oldRoles.create( new RoleRecord( "custom", "alice" ) );

        SystemGraphRealm realm = startSystemGraphRealm();

        // Give Alice match privileges in 'neo4j'
        ResourcePrivilege readPrivilege = new ResourcePrivilege( GRANT, READ, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        ResourcePrivilege findPrivilege = new ResourcePrivilege( GRANT, TRAVERSE, new Resource.GraphResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        GraphDatabaseService systemDB = dbManager.getManagementService().database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = systemDB.beginTx() )
        {
            transaction.execute( String.format( "GRANT MATCH {*} ON GRAPH %s NODES * TO %s", DEFAULT_DATABASE_NAME, "custom" ) );
            transaction.commit();
        }

        assertAuthenticationSucceeds( realm, "alice", "bar" );
        Set<ResourcePrivilege> privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( readPrivilege, findPrivilege ) );

        realm.stop();

        // Create a new database 'foo' and set it to default db in config
        dbManager.getManagementService().createDatabase( "foo" );
        defaultConfig.set( default_database, "foo" );

        realm.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realm, "alice", "bar" );

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
        assertAuthenticationSucceeds( realm, "alice", "bar" );

        // Alice should still have read privileges in 'neo4j'
        privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( readPrivilege, findPrivilege ) );
    }

    private SystemGraphRealm startSystemGraphRealm() throws Exception
    {
        Config config = Config.defaults( DatabaseManagementSystemSettings.auth_store_directory, testDirectory.directory( "data/dbms" ).toPath() );
        EnterpriseSystemGraphInitializer systemGraphInitializer = new EnterpriseSystemGraphInitializer( dbManager, config );
        EnterpriseSecurityGraphInitializer securityGraphInitializer =
                new EnterpriseSecurityGraphInitializer( dbManager, systemGraphInitializer, securityLog, oldUsers, oldRoles, initialPassword, defaultAdmin,
                        secureHasher );

        RateLimitedAuthenticationStrategy authenticationStrategy = new RateLimitedAuthenticationStrategy( Clock.systemUTC(), config );
        SystemGraphRealm realm = new SystemGraphRealm( securityGraphInitializer, dbManager, secureHasher, authenticationStrategy, true, true );
        realm.initialize();
        realm.start();
        return realm;
    }

    private AssertableLogProvider.LogMatcher info( String message, String... arguments )
    {
        if ( arguments.length == 0 )
        {
            return inLog( this.getClass() ).info( message );
        }
        return inLog( this.getClass() ).info( message, (Object[]) arguments );
    }

    private static class TestDatabaseManager extends BasicSystemGraphRealmTestHelper.TestDatabaseManager
    {
        TestDatabaseManager( TestDirectory testDir )
        {
            super( testDir );
        }

        @Override
        protected DatabaseManagementService createManagementService( TestDirectory testDir )
        {
            return new TestEnterpriseDatabaseManagementServiceBuilder( testDir.homeDir() ).impermanent()
                                                                                .setConfig( GraphDatabaseSettings.auth_enabled, false ).build();
        }

        boolean userHasRole( String user, String role )
        {
            List<Object> usersWithRole = new LinkedList<>();
            try ( Transaction tx = testSystemDb.beginTx() )
            {
                Result result = tx.execute( "SHOW USERS" );
                result.stream().filter( u -> ((List) u.get( "roles" )).contains( role ) ).map( u -> u.get( "user" ) ).forEach( usersWithRole::add );
                tx.commit();
                result.close();
            }

            return usersWithRole.contains( user );
        }
    }
}
