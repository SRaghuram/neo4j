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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    void shouldShareRoleNodeBetweenUsersWithSameRole() throws Exception
    {
        long roleNodeCount = nbrOfRoleNodes();
        long userNodeCount = nbrOfUserNodes();
        setupTwoReaders();
        assertEquals( roleNodeCount, nbrOfRoleNodes() );
        assertEquals( userNodeCount + 2, nbrOfUserNodes() );
    }

    @Test
    void shouldFailShowPrivilegeForUnknownUser()
    {
        assertException( () -> realm.showPrivilegesForUser( "TomRiddle" ), InvalidArgumentsException.class, "User 'TomRiddle' does not exist." );
        assertException( () -> realm.showPrivilegesForUser( "" ), InvalidArgumentsException.class, "User '' does not exist." );
        assertException( () -> realm.showPrivilegesForUser( "Neo," ), InvalidArgumentsException.class, "User 'Neo,' does not exist." );
    }

    @Test
    void shouldSilentlyIgnoreAlreadyGrantedPrivilege() throws Exception
    {
        // Given
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newRole( "custom", "Neo" );
        DatabasePrivilege dbPriv1 = new DatabasePrivilege( "*" );
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.READ, Resource.GRAPH ) );
        realm.grantPrivilegeToRole( "custom", dbPriv1 );
        long privilegeNodes = nbrOfPrivilegeNodes();
        long resourceNodes = nbrOfResourceNodes();

        // When
        realm.grantPrivilegeToRole( "custom", dbPriv1 );

        // Then
        assertThat( privilegeNodes, equalTo( nbrOfPrivilegeNodes() ) );
        assertThat( resourceNodes, equalTo( nbrOfResourceNodes() ) );
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
}
