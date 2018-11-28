/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.ExcessiveAttemptsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.AuthToken;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import org.neo4j.server.security.enterprise.auth.RoleRecord;
import org.neo4j.server.security.enterprise.auth.RoleRepository;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.ShiroAuthToken;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.server.security.enterprise.log.SecurityLog;
import org.neo4j.string.UTF8;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class SystemGraphRealmTest
{
    TestDatabaseManager dbManager;
    ContextSwitchingSystemGraphQueryExecutor executor;
    private AssertableLogProvider log;
    private SecurityLog securityLog;
    private int maxFailedAttemps;

    @BeforeEach
    void setUp()
    {
        dbManager = new TestDatabaseManager();
        executor = new TestContextSwitchingSystemGraphQueryExecutor( dbManager );
        log = new AssertableLogProvider();
        securityLog = new SecurityLog( log.getLog( getClass() ) );
        maxFailedAttemps = Config.defaults().get( GraphDatabaseSettings.auth_max_failed_attempts );
    }

    @AfterEach
    void tearDown()
    {
        dbManager.testSystemDb.shutdown();
    }

    @Test
    void shouldImportExplicitAdmin() throws Throwable
    {
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldPerformImport()
                .mayNotPerformMigration()
                .importUsers( "alice" )
                .importRole( PredefinedRoles.ADMIN, "alice" )
                .build()
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
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .migrateRole( "goon", "bob" )
                .build()
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
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( UserManager.INITIAL_USER_NAME )
                .build()
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
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( "jane", "joe" )
                .build()
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
    void shouldSetDefaultAdmin() throws Throwable
    {
        // Given existing users but no admin
        try
        {
            prePopulateUsers( "alice", "bob", "trinity" );
            fail();
        }
        catch ( InvalidArgumentsException e )
        {
            assertThat( e.getMessage(), startsWith( "No roles defined, and cannot determine which user should be admin" ) );
        }

        // When a default admin is set by command
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .defaultAdmins( "trinity" )
                .build()
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
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldPerformImport()
                .mayNotPerformMigration()
                .importUsers( "alice" )
                .importRole( "not_admin", "alice" )
                .build()
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
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .migrateUsers( "alice", "bob" )
                .migrateRole( PredefinedRoles.ADMIN, "alice" )
                .build() );

        // First make sure one of the users will have a cached successful authentication result for variation
        assertAuthenticationSucceeds( realm, "alice" );

        assertAuthenticationFailsWithTooManyAttempts( realm, "alice", maxFailedAttemps + 1 );
        assertAuthenticationFailsWithTooManyAttempts( realm, "bob", maxFailedAttemps + 1 );
    }

    private void prePopulateUsers( String... usernames ) throws Throwable
    {
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldPerformImport()
                .importUsers( usernames )
                .build()
        );
        realm.stop();
        realm.shutdown();
    }

    private static class ImportOptionsBuilder
    {
        private boolean shouldPerformImport;
        private boolean mayPerformMigration;
        private boolean shouldPurgeImportRepositoriesAfterSuccesfulImport;
        private boolean shouldResetSystemGraphAuthBeforeImport;
        private String[] importUsers = new String[0];
        private List<Pair<String,String[]>> importRoles = new ArrayList<>();
        private String[] migrateUsers = new String[0];
        private List<Pair<String,String[]>> migrateRoles = new ArrayList<>();
        private String[] initialUsers = new String[0];
        private String[] defaultAdmins = new String[0];

        private ImportOptionsBuilder()
        {
        }

        ImportOptionsBuilder shouldPerformImport()
        {
            shouldPerformImport = true;
            return this;
        }

        ImportOptionsBuilder shouldNotPerformImport()
        {
            shouldPerformImport = false;
            return this;
        }

        ImportOptionsBuilder mayPerformMigration()
        {
            mayPerformMigration = true;
            return this;
        }

        ImportOptionsBuilder mayNotPerformMigration()
        {
            mayPerformMigration = false;
            return this;
        }

        ImportOptionsBuilder shouldPurgeImportRepositoriesAfterSuccesfulImport()
        {
            shouldPurgeImportRepositoriesAfterSuccesfulImport = true;
            return this;
        }

        ImportOptionsBuilder shouldNotPurgeImportRepositoriesAfterSuccesfulImport()
        {
            shouldPurgeImportRepositoriesAfterSuccesfulImport = false;
            return this;
        }

        ImportOptionsBuilder shouldResetSystemGraphAuthBeforeImport()
        {
            shouldResetSystemGraphAuthBeforeImport = true;
            return this;
        }

        ImportOptionsBuilder shouldNotResetSystemGraphAuthBeforeImport()
        {
            shouldResetSystemGraphAuthBeforeImport = false;
            return this;
        }

        ImportOptionsBuilder importUsers( String... importUsers )
        {
            this.importUsers = importUsers;
            return this;
        }

        ImportOptionsBuilder importRole( String role, String... users )
        {
            this.importRoles.add( Pair.of( role, users ) );
            return this;
        }

        ImportOptionsBuilder migrateUsers( String... migrateUsers )
        {
            this.migrateUsers = migrateUsers;
            return this;
        }

        ImportOptionsBuilder migrateRole( String role, String... users )
        {
            this.migrateRoles.add( Pair.of( role, users ) );
            return this;
        }

        ImportOptionsBuilder initialUsers( String... initialUsers )
        {
            this.initialUsers = initialUsers;
            return this;
        }

        ImportOptionsBuilder defaultAdmins( String... defaultAdmins )
        {
            this.defaultAdmins = defaultAdmins;
            return this;
        }

        @SuppressWarnings( "unchecked" )
        SystemGraphImportOptions build() throws IOException, InvalidArgumentsException
        {
            return testImportOptions(
                    shouldPerformImport,
                    mayPerformMigration,
                    shouldPurgeImportRepositoriesAfterSuccesfulImport,
                    shouldResetSystemGraphAuthBeforeImport,
                    importUsers,
                    importRoles.toArray( new Pair[0] ),
                    migrateUsers,
                    migrateRoles.toArray( new Pair[0] ),
                    initialUsers,
                    defaultAdmins );
        }
    }

    private static SystemGraphImportOptions testImportOptions(
            boolean shouldPerformImport,
            boolean mayPerformMigration,
            boolean shouldPurgeImportRepositoriesAfterSuccesfulImport,
            boolean shouldResetSystemGraphAuthBeforeImport,
            String[] importUsers,
            Pair<String,String[]>[] importRoles,
            String[] migrateUsers,
            Pair<String,String[]>[] migrateRoles,
            String[] initialUsers,
            String[] defaultAdmins
    ) throws IOException, InvalidArgumentsException
    {
        UserRepository importUserRepository = new InMemoryUserRepository();
        RoleRepository importRoleRepository = new InMemoryRoleRepository();
        UserRepository migrationUserRepository = new InMemoryUserRepository();
        RoleRepository migrationRoleRepository = new InMemoryRoleRepository();
        UserRepository initialUserRepository = new InMemoryUserRepository();
        UserRepository defaultAdminRepository = new InMemoryUserRepository();

        populateUserRepository( importUserRepository, importUsers );
        populateRoleRepository( importRoleRepository, importRoles );
        populateUserRepository( migrationUserRepository, migrateUsers );
        populateRoleRepository( migrationRoleRepository, migrateRoles );
        populateUserRepository( initialUserRepository, initialUsers );
        populateUserRepository( defaultAdminRepository, defaultAdmins );

        return new SystemGraphImportOptions(
                shouldPerformImport,
                mayPerformMigration,
                shouldPurgeImportRepositoriesAfterSuccesfulImport,
                shouldResetSystemGraphAuthBeforeImport,
                () -> importUserRepository,
                () -> importRoleRepository,
                () -> migrationUserRepository,
                () -> migrationRoleRepository,
                () -> initialUserRepository,
                () -> defaultAdminRepository
        );
    }

    private static void populateUserRepository( UserRepository repository, String[] usernames ) throws IOException, InvalidArgumentsException
    {
        for ( String username : usernames )
        {
            // Use username as password to simplify test assertions
            User user = new User.Builder( username, LegacyCredential.forPassword( username ) ).build();
            repository.create( user );
        }
    }

    private static void populateRoleRepository( RoleRepository repository, Pair<String,String[]>[] roles ) throws IOException, InvalidArgumentsException
    {
        for ( Pair<String,String[]> role : roles )
        {
            RoleRecord roleRecord = new RoleRecord( role.first(), role.other() );
            repository.create( roleRecord );
        }
    }

    private SystemGraphRealm testRealmWithImportOptions( SystemGraphImportOptions importOptions ) throws Throwable
    {
        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( executor, secureHasher );
        SystemGraphRealm realm = new SystemGraphRealm(
                systemGraphOperations,
                new SystemGraphInitializer( executor, systemGraphOperations, importOptions, secureHasher, securityLog ),
                new SecureHasher(),
                new BasicPasswordPolicy(),
                newRateLimitedAuthStrategy(),
                true,
                true
        );

        realm.initialize();
        realm.start();

        return realm;
    }

    private static AuthenticationStrategy newRateLimitedAuthStrategy()
    {
        return new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() );
    }

    private class TestContextSwitchingSystemGraphQueryExecutor extends ContextSwitchingSystemGraphQueryExecutor
    {
        private TestThreadToStatementContextBridge bridge;

        TestContextSwitchingSystemGraphQueryExecutor( DatabaseManager databaseManager )
        {
            super( databaseManager, DEFAULT_DATABASE_NAME );
            bridge = new TestThreadToStatementContextBridge();
        }

        @Override
        protected ThreadToStatementContextBridge getThreadToStatementContextBridge()
        {
            return bridge;
        }
    }

    private class TestDatabaseManager extends LifecycleAdapter implements DatabaseManager
    {
        GraphDatabaseFacade testSystemDb;

        TestDatabaseManager()
        {
            testSystemDb = (GraphDatabaseFacade) new TestEnterpriseGraphDatabaseFactory()
                    .newImpermanentDatabaseBuilder( new File( "target/test-data/system-graph-test-db" ) )
                    .setConfig( GraphDatabaseSettings.auth_enabled, "false" )
                    .newGraphDatabase();
        }

        @Override
        public Optional<DatabaseContext> getDatabaseContext( String name )
        {
            if ( SYSTEM_DATABASE_NAME.equals( name ) )
            {
                DependencyResolver dependencyResolver = testSystemDb.getDependencyResolver();
                Database database = dependencyResolver.resolveDependency( Database.class );
                return Optional.of( new DatabaseContext( database, testSystemDb ) );
            }
            return Optional.empty();
        }

        @Override
        public DatabaseContext createDatabase( String name )
        {
            throw new UnsupportedOperationException( "Call to createDatabase not expected" );
        }

        @Override
        public void shutdownDatabase( String name )
        {
        }

        @Override
        public List<String> listDatabases()
        {
            return emptyList();
        }
    }

    private class TestThreadToStatementContextBridge extends ThreadToStatementContextBridge
    {
        TestThreadToStatementContextBridge()
        {
            super( null );
        }

        @Override
        public boolean hasTransaction()
        {
            return false;
        }

        @Override
        public KernelTransaction getKernelTransactionBoundToThisThread( boolean strict )
        {
            return null;
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
            try
            {
                assertNull( realm.getAuthenticationInfo( testAuthenticationToken( username, username ) ) );
            }
            catch ( AuthenticationException e )
            {
                // This is expected
            }
        }

        // Also test the non-cached result explicitly
        try
        {
            assertNull( realm.doGetAuthenticationInfo( testAuthenticationToken( username, username ) ) );
        }
        catch ( AuthenticationException e )
        {
            // This is expected
        }
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
