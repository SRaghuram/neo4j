/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Resource;
import com.neo4j.server.security.enterprise.auth.SecureHasher;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.ImportOptionsBuilder;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphImportOptions;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphInitializer;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphOperations;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.File;
import java.util.Set;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.auth.AuthenticationStrategy;
import org.neo4j.server.security.auth.BasicPasswordPolicy;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.test.assertion.Assert.assertException;

@ExtendWith( TestDirectoryExtension.class )
class SystemGraphInternalsTest
{
    private GraphDatabaseService database;
    private ContextSwitchingSystemGraphQueryExecutor systemGraphExecutor;
    private SystemGraphRealm realm;
    private String activeDbName;

    @Inject
    private TestDirectory testDirectory;

    @BeforeEach
    void setUp() throws Throwable
    {
        TestCommercialGraphDatabaseFactory factory = new TestCommercialGraphDatabaseFactory();
        File storeDir = testDirectory.databaseDir();
        final GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( storeDir );
        builder.setConfig( SecuritySettings.auth_provider, SecuritySettings.NATIVE_REALM_NAME );
        database = builder.newGraphDatabase();
        activeDbName = ((GraphDatabaseFacade) database).databaseLayout().getDatabaseName();
        DatabaseManager<?> databaseManager = getDatabaseManager();
        systemGraphExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, activeDbName );
        setupSystemGraphRealm();

        // Neo4j default user should have a DbRole node connected it to admin role already
        assertEquals( 1, nbrOfDbRoleNodes() );
    }

    private void setupSystemGraphRealm() throws Throwable
    {
        AssertableLogProvider log = new AssertableLogProvider();
        SecurityLog securityLog = new SecurityLog( log.getLog( getClass() ) );

        SystemGraphImportOptions importOptions = new ImportOptionsBuilder().build();

        SecureHasher secureHasher = new SecureHasher();
        SystemGraphOperations systemGraphOperations = new SystemGraphOperations( systemGraphExecutor, secureHasher );
        realm = new SystemGraphRealm(
                systemGraphOperations,
                new SystemGraphInitializer( systemGraphExecutor, systemGraphOperations, importOptions, secureHasher, securityLog ),
                true,
                new SecureHasher(),
                new BasicPasswordPolicy(),
                Mockito.mock( AuthenticationStrategy.class ),
                true,
                true
        );
        realm.initialize();
        realm.start(); // creates default user and roles
    }

    @AfterEach
    void tearDown()
    {
        if ( database != null )
        {
            database.shutdown();
            database = null;
        }
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    @Test
    void shouldNotShareDbRoleNodeBetweenUsersWithSameRole() throws Exception
    {
        setupTwoReaders();
        assertEquals( 3, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenUserIsDeleted() throws Exception
    {
        setupTwoReaders();

        realm.deleteUser( "Neo" );
        assertEquals( 2, nbrOfDbRoleNodes() );

        realm.deleteUser( "Trinity" );
        assertEquals( 1,    nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenUserIsUnassigned() throws Exception
    {
       setupTwoReaders();

       realm.removeRoleFromUser( READER, "Neo" );
       assertEquals( 2, nbrOfDbRoleNodes() );

       realm.removeRoleFromUser( READER, "Trinity" );
       assertEquals( 1, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenDeletingRole() throws Exception
    {
        realm.newRole( "tmpRole", "neo4j" );
        assertEquals( 2, nbrOfDbRoleNodes() );

        realm.deleteRole( "tmpRole" );
        assertEquals( 1, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldFailShowPrivilegeForUnknownUser() throws Exception
    {
        // this just returns an empty result... should probably generate an error message about the user not existing
        realm.showPrivilegesForUser( "TomRiddle" );

        // these throw because the user name is not valid
        assertException( () -> realm.showPrivilegesForUser( "" ), InvalidArgumentsException.class, "The provided username is empty." );
        assertException( () -> realm.showPrivilegesForUser( "Neo," ), InvalidArgumentsException.class,
                "Username 'Neo,' contains illegal characters. Use ascii characters that are not ',', ':' or whitespaces." );
    }

    @Test
    void shouldShowPrivilegesForUser() throws Exception
    {
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newRole( "custom", "Neo" );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "Neo" );
        assertTrue( privileges.isEmpty() );

        realm.grantPrivilegeToRole( "custom", new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        privileges = realm.showPrivilegesForUser( "Neo" );
        DatabasePrivilege databasePrivilege = new DatabasePrivilege( activeDbName );
        databasePrivilege.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        assertThat( privileges, containsInAnyOrder( databasePrivilege ) );
    }

    @Test
    void shouldShowAdminPrivileges() throws Exception
    {
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newRole( "custom", "Neo" );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "Neo" );
        assertTrue( privileges.isEmpty() );

        realm.setAdmin( "custom", true );
        privileges = realm.showPrivilegesForUser( "Neo" );
        DatabasePrivilege databasePrivilege = new DatabasePrivilege( activeDbName );
        databasePrivilege.setAdmin();
        assertThat( privileges, containsInAnyOrder( databasePrivilege ) );

        realm.setAdmin( "custom", false );
        privileges = realm.showPrivilegesForUser( "Neo" );
        assertTrue( privileges.isEmpty() );
    }

    private void setupTwoReaders() throws InvalidArgumentsException
    {
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newUser( "Trinity", password( "123" ), false );
        realm.addRoleToUser( READER, "Neo" );
        realm.addRoleToUser( READER, "Trinity" );
    }

    private long nbrOfDbRoleNodes()
    {
        String query = "MATCH (dbr:DbRole) RETURN count(dbr)";
        return systemGraphExecutor.executeQueryLong( query );
    }
}
