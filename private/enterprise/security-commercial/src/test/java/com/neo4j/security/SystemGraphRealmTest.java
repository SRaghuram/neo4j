/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.dbms.database.MultiDatabaseManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.security.UserManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.security.auth.LegacyCredential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.server.security.auth.InMemoryUserRepository;
import org.neo4j.server.security.auth.RateLimitedAuthenticationStrategy;
import org.neo4j.server.security.auth.UserRepository;
import org.neo4j.server.security.enterprise.auth.InMemoryRoleRepository;
import org.neo4j.server.security.enterprise.auth.RoleRecord;
import org.neo4j.server.security.enterprise.auth.RoleRepository;
import org.neo4j.server.security.enterprise.auth.SecureHasher;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SystemGraphRealmTest
{
    TestDatabaseManager dbManager;
    SystemGraphExecutor executor;
    List<String> messages;

    @BeforeEach
    void setUp()
    {
        dbManager = new TestDatabaseManager();
        executor =  new TestSystemGraphExecutor( dbManager );
        messages = new ArrayList<>();
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
    }

    // TODO: This is not allowed in InternalFlatFileRealm. There you can only set UserManager.INITIAL_USER_NAME
    // Should we actively prevent this case even though the admin tool currently does not support it?
    @Test
    void shouldSetInitialUsersAsAdminWithCustomUsernames() throws Throwable
    {
        SystemGraphRealm realm = testRealmWithImportOptions( new ImportOptionsBuilder()
                .shouldNotPerformImport()
                .mayPerformMigration()
                .initialUsers( "jane", "joe" )
                .build()
        );

        assertThat( realm.getUsernamesForRole( PredefinedRoles.ADMIN ), contains( "jane", "joe" ) );
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

        @SuppressWarnings("unchecked")
        SystemGraphImportOptions build() throws IOException, InvalidArgumentsException
        {
            return testImportOptions(
                    shouldPerformImport,
                    mayPerformMigration,
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
        SystemGraphRealm realm = new SystemGraphRealm(
                executor,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                newRateLimitedAuthStrategy(),
                true,
                true,
                importOptions
        );

        realm.initialize();
        realm.start();

        return realm;
    }

    private static AuthenticationStrategy newRateLimitedAuthStrategy()
    {
        return new RateLimitedAuthenticationStrategy( Clock.systemUTC(), Config.defaults() );
    }

    private class TestSystemGraphExecutor extends SystemGraphExecutor
    {
        private TestThreadToStatementContextBridge bridge;

        TestSystemGraphExecutor( DatabaseManager databaseManager )
        {
            super( databaseManager, DatabaseManager.DEFAULT_DATABASE_NAME );
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
        public Optional<GraphDatabaseFacade> getDatabaseFacade( String name )
        {
            if ( MultiDatabaseManager.SYSTEM_DB_NAME.equals( name ) )
            {
                return Optional.of( testSystemDb );
            }
            return Optional.empty();
        }

        @Override
        public GraphDatabaseFacade createDatabase( String name )
        {
            throw new UnsupportedOperationException( "Call to createDatabase not expected" );
        }

        @Override
        public void shutdownDatabase( String name )
        {
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

    private void log( String format, String... params )
    {
        messages.add( String.format( format, params ) );
    }
}
