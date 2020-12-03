/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.neo4j.dbms.EnterpriseSystemGraphComponent;
import com.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.SpecialDatabase;
import com.neo4j.server.security.enterprise.auth.RoleRecord;
import com.neo4j.server.security.enterprise.auth.TestExecutorCaffeineCacheFactory;
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
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DefaultSystemGraphInitializer;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.database.SystemGraphInitializer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.security.FunctionSegment;
import org.neo4j.internal.kernel.api.security.LabelSegment;
import org.neo4j.internal.kernel.api.security.ProcedureSegment;
import org.neo4j.internal.kernel.api.security.RelTypeSegment;
import org.neo4j.internal.kernel.api.security.Segment;
import org.neo4j.internal.kernel.api.security.UserSegment;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.security.BasicSystemGraphRealmTestHelper;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.systemgraph.SystemGraphRealmHelper;
import org.neo4j.server.security.systemgraph.UserSecurityGraphComponent;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.server.security.enterprise.auth.ResourcePrivilege.GrantOrDeny.GRANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.dbms.database.ComponentVersion.SECURITY_USER_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.Status.CURRENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.ACCESS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.CONSTRAINT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.DBMS_ACTIONS;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.EXECUTE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.INDEX;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.MATCH;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.START_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.STOP_DATABASE;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TOKEN;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.TRANSACTION_MANAGEMENT;
import static org.neo4j.internal.kernel.api.security.PrivilegeAction.WRITE;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_PASSWORD;
import static org.neo4j.kernel.api.security.AuthManager.INITIAL_USER_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.AssertableLogProvider.Level.INFO;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.security.BasicSystemGraphRealmTestHelper.assertAuthenticationFails;
import static org.neo4j.security.BasicSystemGraphRealmTestHelper.assertAuthenticationSucceeds;
import static org.neo4j.security.BasicSystemGraphRealmTestHelper.createUser;
import static org.neo4j.server.security.systemgraph.UserSecurityGraphComponentVersion.LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION;

@TestDirectoryExtension
class SystemGraphRealmIT
{
    private TestDatabaseManager dbManager;
    private SystemGraphRealmHelper realmHelper;
    private AssertableLogProvider logProvider;
    private SecurityLog securityLog;
    private Config defaultConfig;

    @Inject
    private TestDirectory testDirectory;

    private InMemoryUserRepository oldUsers;
    private InMemoryRoleRepository oldRoles;
    private InMemoryUserRepository initialPassword;
    private InMemoryUserRepository defaultAdmin;
    private SystemGraphRealm realm;
    private SystemGraphInitializer systemGraphInitializer;

    @BeforeEach
    void setUp()
    {
        SecureHasher secureHasher = new SecureHasher();
        dbManager = new TestDatabaseManager( testDirectory );
        realmHelper = new SystemGraphRealmHelper( SystemGraphRealmHelper.makeSystemSupplier( dbManager ), secureHasher );
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

        startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( "alice", PredefinedRoles.ADMIN ) );
        assertTrue( dbManager.userHasRole( "bob", "goon" ) );
        assertAuthenticationSucceeds( realmHelper, "alice", "foo" );
        assertAuthenticationSucceeds( realmHelper, "bob", "bar", true );
        assertThat( logProvider )
                .containsMessageWithArguments(  "Completed migration of %s %s into system graph.", "2", "users" )
                .containsMessageWithArguments(  "Completed migration of %s %s into system graph.", "2", "roles" );
    }

    @Test
    void shouldHaltMigrationWhenPublicRoleExists() throws Throwable
    {
        oldUsers.create( createUser( "alice", "foo", false ) );
        oldUsers.create( createUser( "bob", "bar", true ) );

        oldRoles.create( new RoleRecord( PredefinedRoles.ADMIN, "alice" ) );
        oldRoles.create( new RoleRecord( "PUBLIC", "bob" ) );

        Exception exception = assertThrows( IllegalStateException.class, this::startSystemGraphRealm );
        assertThat( exception.getMessage(), containsString( "Automatic migration of users and roles into system graph failed because 'PUBLIC' role exists." +
                                                            " Please remove or rename that role and start again." ) );

        assertThat( logProvider )
                .forLevel( ERROR )
                .containsMessages( "Automatic migration of users and roles into system graph failed because 'PUBLIC' role exists. " +
                                   "Please remove or rename that role and start again." );
    }

    @Test
    void shouldCreateDefaultAdminWithPredefinedUsername() throws Throwable
    {
        startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true );
        assertThat( logProvider ).forLevel( INFO )
                .containsMessageWithArguments(  "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, INITIAL_USER_NAME );
    }

    @Test
    void shouldSetInitialUserAsAdminWithChangedPassword() throws Throwable
    {
        initialPassword.create( createUser( INITIAL_USER_NAME, "neo4j1", false ) );

        startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "neo4j1" );
    }

    @Test
    void shouldSetInitialUserWithPasswordChangeRequired() throws Throwable
    {
        initialPassword.create( createUser( INITIAL_USER_NAME, "neo4j1", true ) );

        startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN ) );
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "neo4j1", true );
    }

    @Test
    void shouldLoadInitialUserWithInitialPasswordOnRestart() throws Throwable
    {
        // Given
        startSystemGraphRealm();
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true );

        initialPassword.create( createUser( INITIAL_USER_NAME, "abc123", false ) );
        logProvider.clear();

        // When
        systemGraphInitializer.start();

        // Then
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "abc123" );
        assertThat( logProvider ).forLevel( INFO )
                                 .containsMessageWithArguments( "Updating the initial password in component '%s'  ",
                                         SECURITY_USER_COMPONENT, LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION, CURRENT )
                                 .containsMessageWithArguments( "Updating initial user password from `auth.ini` file: %s", INITIAL_USER_NAME );
    }

    @Test
    void shouldNotLoadInitialUserWithInitialPasswordOnRestartWhenAlreadyChanged() throws Throwable
    {
        // Given
        startSystemGraphRealm();

        initialPassword.create( createUser( INITIAL_USER_NAME, "foo", false ) );

        systemGraphInitializer.start();

        // When
        initialPassword.clear();
        initialPassword.create( createUser( INITIAL_USER_NAME, "bar", false ) );
        logProvider.clear();
        systemGraphInitializer.start();

        // Then
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD );
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, "foo" );
        assertAuthenticationFails( realmHelper, INITIAL_USER_NAME, "bar" );

        assertThat( logProvider ).forLevel( INFO )
                                 .containsMessageWithArguments( "Updating the initial password in component '%s'  ",
                                         SECURITY_USER_COMPONENT, LATEST_COMMUNITY_SECURITY_COMPONENT_VERSION, CURRENT )
                                 .doesNotContainMessageWithArguments( "Updating initial user password from `auth.ini` file: %s", INITIAL_USER_NAME );
    }

    @Test
    void shouldThrowOnBrokenInitialUsersFile() throws Throwable
    {
        // When
        initialPassword.create( createUser( "jane", "foo", false ) );

        // Then
        IllegalStateException wrongUsernameException = assertThrows( IllegalStateException.class, this::startSystemGraphRealm );
        String wrongUsernameErrorMessage = "Invalid `auth.ini` file: the user in the file is not named " + INITIAL_USER_NAME;
        assertThat( wrongUsernameException.getMessage(), containsString( wrongUsernameErrorMessage ) );
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR ).containsMessages( wrongUsernameErrorMessage );

        // Given
        initialPassword.clear();
        logProvider.clear();

        // When
        initialPassword.create( createUser( "jane", "foo", false ) );
        initialPassword.create( createUser( INITIAL_USER_NAME, "foo", false ) );

        // Then
        IllegalStateException multipleUsersErrorException = assertThrows( IllegalStateException.class, this::startSystemGraphRealm );
        String multipleUsersErrorMessage = "Invalid `auth.ini` file: the file contains more than one user";
        assertThat( multipleUsersErrorException.getMessage(), containsString( multipleUsersErrorMessage ) );
        assertThat( logProvider ).forClass( getClass() ).forLevel( ERROR ).containsMessages( multipleUsersErrorMessage );
    }

    @Test
    void shouldMigrateOnlyUserAsAdminEvenWithoutRolesFile() throws Throwable
    {
        oldUsers.create( createUser( "jane", "doe", false ) );

        startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( "jane", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realmHelper, "jane", "doe" );
        assertThat( logProvider ).forLevel( INFO )
                .containsMessageWithArguments( "Completed migration of %s %s into system graph.", "1", "user" )
                .containsMessageWithArguments( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "jane" );
    }

    @Test
    void shouldMigrateDefaultAdminWithMultipleExistingUsers() throws Throwable
    {
        oldUsers.create( createUser( "jane", "doe", false ) );
        oldUsers.create( createUser( "alice", "foo", false ) );
        oldUsers.create( createUser( INITIAL_USER_NAME, "bar", false ) );

        startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( "neo4j", PredefinedRoles.ADMIN ) );
        assertAuthenticationSucceeds( realmHelper, "jane", "doe" );
        assertThat( logProvider ).forLevel( INFO )
                .containsMessageWithArguments( "Completed migration of %s %s into system graph.", "3", "users" )
                .containsMessageWithArguments( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "neo4j" );
    }

    @Test
    void shouldNotMigrateMultipleExistingUsersAsAdminWithCustomUsernames() throws Throwable
    {
        oldUsers.create( createUser( "jane", "doe", false ) );
        oldUsers.create( createUser( "alice", "foo", false ) );

        IllegalStateException exception = assertThrows( IllegalStateException.class, this::startSystemGraphRealm );
        assertThat( exception.getMessage(), containsString( "No roles defined, and cannot determine which user should be admin" ) );
    }

    @Test
    void shouldSetDefaultAdmin() throws Throwable
    {
        oldUsers.create( createUser( "alice", "foo", false ) );
        oldUsers.create( createUser( "bob", "bar", false ) );
        oldUsers.create( createUser( "trinity", "abc", false ) );

        // Given existing users but no admin
        IllegalStateException exception = assertThrows( IllegalStateException.class, this::startSystemGraphRealm );
        assertThat( exception.getMessage(), containsString( "No roles defined, and cannot determine which user should be admin" ) );
        assertThat( logProvider ).forLevel( INFO )
                                 .containsMessageWithArguments( "Completed migration of %s %s into system graph.", "3", "users" );
        assertThat( logProvider ).forLevel( ERROR )
                                 .containsMessages( "No roles defined, and cannot determine which user should be admin" );

        logProvider.clear();
        // When a default admin is set by command
        defaultAdmin.create( createUser( "trinity", "ignored", false ) );

        startSystemGraphRealm();

        // Then
        assertAuthenticationSucceeds( realmHelper, "trinity", "abc" );
        assertTrue( dbManager.userHasRole( "trinity", PredefinedRoles.ADMIN ) );
        assertThat( logProvider ).forLevel( INFO )
                .containsMessageWithArguments( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, "trinity" );
    }

    @Test
    void shouldGetDefaultPrivilegesForDefaultRoles() throws Throwable
    {
        startSystemGraphRealm();

        ResourcePrivilege defaultAccessPrivilege =
                new ResourcePrivilege( GRANT, ACCESS, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.DEFAULT );
        ResourcePrivilege executeProcedurePrivilege =
                new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), ProcedureSegment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege executeFunctionPrivilege =
                new ResourcePrivilege( GRANT, EXECUTE, new Resource.DatabaseResource(), FunctionSegment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege accessPrivilege =
                new ResourcePrivilege( GRANT, ACCESS, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege matchNodePrivilege =
                new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege matchRelPrivilege =
                new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), RelTypeSegment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege writeNodePrivilege =
                new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), LabelSegment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege writeRelPrivilege =
                new ResourcePrivilege( GRANT, WRITE, new Resource.GraphResource(), RelTypeSegment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege tokenNodePrivilege =
                new ResourcePrivilege( GRANT, TOKEN, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege indexNodePrivilege =
                new ResourcePrivilege( GRANT, INDEX, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege constraintNodePrivilege =
                new ResourcePrivilege( GRANT, CONSTRAINT, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege startDbPrivilege =
                new ResourcePrivilege( GRANT, START_DATABASE, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege stopDbPrivilege =
                new ResourcePrivilege( GRANT, STOP_DATABASE, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege transactionPrivilege =
                new ResourcePrivilege( GRANT, TRANSACTION_MANAGEMENT, new Resource.DatabaseResource(), UserSegment.ALL, SpecialDatabase.ALL );
        ResourcePrivilege dbmsPrivilege =
                new ResourcePrivilege( GRANT, DBMS_ACTIONS, new Resource.DatabaseResource(), Segment.ALL, SpecialDatabase.ALL );

        assertThat( privilegesFor( realm, PredefinedRoles.READER ),
                containsInAnyOrder( accessPrivilege, matchNodePrivilege, matchRelPrivilege ) );

        assertThat( privilegesFor( realm, PredefinedRoles.PUBLIC ),
                containsInAnyOrder( defaultAccessPrivilege, executeProcedurePrivilege, executeFunctionPrivilege ) );

        assertThat( privilegesFor( realm, PredefinedRoles.EDITOR ),
                containsInAnyOrder( accessPrivilege, matchNodePrivilege, matchRelPrivilege, writeNodePrivilege, writeRelPrivilege ) );

        assertThat( privilegesFor( realm, PredefinedRoles.PUBLISHER ),
                containsInAnyOrder( accessPrivilege, matchNodePrivilege, matchRelPrivilege, writeNodePrivilege, writeRelPrivilege, tokenNodePrivilege ) );

        assertThat( privilegesFor( realm, PredefinedRoles.ARCHITECT ),
                containsInAnyOrder( accessPrivilege, matchNodePrivilege, matchRelPrivilege, writeNodePrivilege, writeRelPrivilege, tokenNodePrivilege,
                        indexNodePrivilege, constraintNodePrivilege ) );

        assertThat( privilegesFor( realm, PredefinedRoles.ADMIN ),
                containsInAnyOrder( accessPrivilege, matchNodePrivilege, matchRelPrivilege, writeNodePrivilege, writeRelPrivilege, tokenNodePrivilege,
                        indexNodePrivilege, constraintNodePrivilege, startDbPrivilege, stopDbPrivilege, transactionPrivilege, dbmsPrivilege ) );
    }

    private Set<ResourcePrivilege> privilegesFor( SystemGraphRealm realm, String role )
    {
        return realm.getPrivilegesForRoles( Collections.singleton( role ) );
    }

    @Test
    void shouldHandleCustomDefaultDatabase() throws Throwable
    {
        defaultConfig.set( default_database, "foo" );

        startSystemGraphRealm();

        assertTrue( dbManager.userHasRole( INITIAL_USER_NAME, PredefinedRoles.ADMIN  ));
        assertAuthenticationSucceeds( realmHelper, INITIAL_USER_NAME, INITIAL_PASSWORD, true );
        assertThat( logProvider ).forLevel( INFO )
                .containsMessageWithArguments( "Assigned %s role to user '%s'.", PredefinedRoles.ADMIN, INITIAL_USER_NAME );
    }

    @Test
    void shouldHandleSwitchOfDefaultDatabase() throws Throwable
    {
        oldUsers.create( createUser( "alice", "bar", false ) );
        oldRoles.create( new RoleRecord( "custom", "alice" ) );

        startSystemGraphRealm();

        // Give Alice match privileges in 'neo4j'
        ResourcePrivilege matchPrivilege = new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, DEFAULT_DATABASE_NAME );
        GraphDatabaseService systemDB = dbManager.getManagementService().database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = systemDB.beginTx() )
        {
            transaction.execute( String.format( "GRANT MATCH {*} ON GRAPH %s NODES * TO %s", DEFAULT_DATABASE_NAME, "custom" ) );
            transaction.commit();
        }

        assertAuthenticationSucceeds( realmHelper, "alice", "bar" );
        Set<ResourcePrivilege> privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( matchPrivilege ) );

        // Create a new database 'foo' and set it to default db in config
        dbManager.getManagementService().createDatabase( "foo" );
        defaultConfig.set( default_database, "foo" );

        systemGraphInitializer.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realmHelper, "alice", "bar" );

        // Alice should still have read privileges in 'neo4j'
        privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( matchPrivilege ) );

        // Alice should NOT have read privileges in 'foo'
        assertFalse( privileges.contains( new ResourcePrivilege( GRANT, MATCH, new Resource.AllPropertiesResource(), LabelSegment.ALL, "foo" ) ) );

        // Switch back default db to 'neo4j'
        defaultConfig.set( default_database, DEFAULT_DATABASE_NAME );

        systemGraphInitializer.start();

        // Alice should still be able to authenticate
        assertAuthenticationSucceeds( realmHelper, "alice", "bar" );

        // Alice should still have read privileges in 'neo4j'
        privileges = realm.getPrivilegesForRoles( Collections.singleton( "custom" ) );
        assertThat( privileges, containsInAnyOrder( matchPrivilege ) );
    }

    private void startSystemGraphRealm() throws Exception
    {
        Config config = Config.defaults( DatabaseManagementSystemSettings.auth_store_directory, testDirectory.directory( "data/dbms" ) );

        UserSecurityGraphComponent userSecurityGraphComponent = new UserSecurityGraphComponent( securityLog, oldUsers, initialPassword, config );
        EnterpriseSecurityGraphComponent enterpriseSecurityGraphComponent =
                new EnterpriseSecurityGraphComponent( securityLog, oldRoles, defaultAdmin, config );

        var systemGraphComponents = new SystemGraphComponents();
        systemGraphComponents.register( new EnterpriseSystemGraphComponent( config ) );
        systemGraphComponents.register( userSecurityGraphComponent );
        systemGraphComponents.register( enterpriseSecurityGraphComponent );
        systemGraphInitializer = new DefaultSystemGraphInitializer( SystemGraphRealmHelper.makeSystemSupplier( dbManager ), systemGraphComponents );
        systemGraphInitializer.start();

        RateLimitedAuthenticationStrategy authenticationStrategy = new RateLimitedAuthenticationStrategy( Clock.systemUTC(), config );
        realm = new SystemGraphRealm( realmHelper, authenticationStrategy, true, true, enterpriseSecurityGraphComponent,
                                      TestExecutorCaffeineCacheFactory.getInstance() );
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
            return new TestEnterpriseDatabaseManagementServiceBuilder( testDir.homePath() )
                    .impermanent()
                    .noOpSystemGraphInitializer()
                    .setConfig( GraphDatabaseSettings.auth_enabled, false )
                    .build();
        }

        boolean userHasRole( String user, String role )
        {
            List<Object> usersWithRole = new LinkedList<>();
            try ( Transaction tx = testSystemDb.beginTx() )
            {
                Result result = tx.execute( "SHOW USERS" );
                //noinspection rawtypes
                result.stream().filter( u -> ((List) u.get( "roles" )).contains( role ) ).map( u -> u.get( "user" ) ).forEach( usersWithRole::add );
                tx.commit();
                result.close();
            }

            return usersWithRole.contains( user );
        }
    }
}
