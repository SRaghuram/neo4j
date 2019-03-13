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
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import com.neo4j.server.security.enterprise.systemgraph.TestSystemGraphRealm;
import com.neo4j.test.TestCommercialGraphDatabaseFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Set;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
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
        String activeDbName = ((GraphDatabaseFacade) database).databaseLayout().getDatabaseName();
        DatabaseManager<?> databaseManager = getDatabaseManager();
        systemGraphExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, activeDbName );
        AssertableLogProvider log = new AssertableLogProvider();
        SecurityLog securityLog = new SecurityLog( log.getLog( getClass() ) );

        realm = TestSystemGraphRealm.testRealm( securityLog, systemGraphExecutor );
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
    void defaultNodes()
    {
        // should have system, default and "*" db
        assertEquals( 3, nbrOfDbNodes() );

        // should have default neo4j user
        assertEquals( 1, nbrOfUserNodes() );

        // should have default roles
        assertEquals( 5, nbrOfRoleNodes() );

        // default privileges
        assertEquals( 5 + 4 + 3 + 2 + 1, nbrOfPrivilegeNodes() );

        // graph, token, schema, system
        assertEquals( 4, nbrOfResourceNodes() );
    }

    @Test
    void shouldNotShareDbRoleNodeBetweenUsersWithSameRole() throws Exception
    {
        long roleNodeCount = nbrOfRoleNodes();
        long userNodeCount = nbrOfUserNodes();
        setupTwoReaders();
        assertEquals( 0, nbrOfDbRoleNodes() );
        assertEquals( roleNodeCount, nbrOfRoleNodes() );
        assertEquals( userNodeCount + 2, nbrOfUserNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenUserIsDeleted() throws Exception
    {
        setupTwoReaders();

        realm.deleteUser( "Neo" );
        assertEquals( 0, nbrOfDbRoleNodes() );

        realm.deleteUser( "Trinity" );
        assertEquals( 0, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenUserIsUnassigned() throws Exception
    {
       setupTwoReaders();

       realm.removeRoleFromUser( READER, "Neo" );
       assertEquals( 0, nbrOfDbRoleNodes() );

       realm.removeRoleFromUser( READER, "Trinity" );
       assertEquals( 0, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldRemoveDbRoleNodeWhenDeletingRole() throws Exception
    {
        realm.newRole( "tmpRole", "neo4j" );
        assertEquals( 0, nbrOfDbRoleNodes() );

        realm.deleteRole( "tmpRole" );
        assertEquals( 0, nbrOfDbRoleNodes() );
    }

    @Test
    void shouldFailShowPrivilegeForUnknownUser()
    {
        assertException( () -> realm.showPrivilegesForUser( "TomRiddle" ), InvalidArgumentsException.class, "User 'TomRiddle' does not exist." );
        assertException( () -> realm.showPrivilegesForUser( "" ), InvalidArgumentsException.class, "User '' does not exist." );
        assertException( () -> realm.showPrivilegesForUser( "Neo," ), InvalidArgumentsException.class, "User 'Neo,' does not exist." );
    }

    @Test
    void shouldShowPrivilegesForUser() throws Exception
    {
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newRole( "custom", "Neo" );

        Set<DatabasePrivilege> privileges = realm.showPrivilegesForUser( "Neo" );
        assertTrue( privileges.isEmpty() );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        realm.grantPrivilegeToRole( "custom", dbPriv );
        privileges = realm.showPrivilegesForUser( "Neo" );
        DatabasePrivilege databasePrivilege = new DatabasePrivilege( "*" );
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
        DatabasePrivilege databasePrivilege = new DatabasePrivilege( "*" );
        databasePrivilege.addPrivilege( new ResourcePrivilege( Action.WRITE, Resource.SYSTEM ) );
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

    private long nbrOfPrivilegeNodes()
    {
        String query = "MATCH (p:Privilege) RETURN count(p)";
        return systemGraphExecutor.executeQueryLong( query );

    }

    private long nbrOfResourceNodes()
    {
        String query = "MATCH (res:Resource) RETURN count(res)";
        return systemGraphExecutor.executeQueryLong( query );
    }

    private long nbrOfDbNodes()
    {
        String query = "MATCH (db:Database) RETURN count(db)";
        return systemGraphExecutor.executeQueryLong( query );
    }

    private long nbrOfRoleNodes()
    {
        String query = "MATCH (role:Role) RETURN count(role)";
        return systemGraphExecutor.executeQueryLong( query );
    }

    private long nbrOfUserNodes()
    {
        String query = "MATCH (u:User) RETURN count(u)";
        return systemGraphExecutor.executeQueryLong( query );
    }

    private long nbrOfDbRoleNodes()
    {
        String query = "MATCH (dbr:DbRole) RETURN count(dbr)";
        return systemGraphExecutor.executeQueryLong( query );
    }
}
