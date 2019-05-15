/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.security;

import com.neo4j.server.security.enterprise.auth.DatabasePrivilege;
import com.neo4j.server.security.enterprise.auth.Resource;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import com.neo4j.server.security.enterprise.auth.Segment;
import com.neo4j.server.security.enterprise.configuration.SecuritySettings;
import com.neo4j.server.security.enterprise.log.SecurityLog;
import com.neo4j.server.security.enterprise.systemgraph.ImportOptionsBuilder;
import com.neo4j.server.security.enterprise.systemgraph.SystemGraphRealm;
import com.neo4j.server.security.enterprise.systemgraph.TestSystemGraphRealm;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.server.security.systemgraph.ContextSwitchingSystemGraphQueryExecutor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.AnyValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;
import static org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles.READER;
import static org.neo4j.test.assertion.Assert.assertException;

@ExtendWith( TestDirectoryExtension.class )
class SystemGraphInternalsTest
{
    private static final long DB_NODE_COUNT = 2;
    private static final long USER_NODE_COUNT = 1;
    private static final long ROLE_NODE_COUNT = 5;
    private static final long PRIVILEGE_NODE_COUNT = 6;
    private static final long PRIVILEGE_ASSIGNMENT_COUNT = 6 + 5 + 4 + 3 + 2;
    private static final long RESOURCE_NODE_COUNT = 4;

    private GraphDatabaseService database;
    private ContextSwitchingSystemGraphQueryExecutor systemGraphExecutor;
    private SystemGraphRealm realm;

    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws Throwable
    {
        File storeDir = testDirectory.storeDir();
        final DatabaseManagementServiceBuilder builder = new TestCommercialDatabaseManagementServiceBuilder( storeDir );
        builder.setConfig( SecuritySettings.auth_provider, SecuritySettings.NATIVE_REALM_NAME );
        managementService = builder.build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
        DatabaseManager<?> databaseManager = getDatabaseManager();
        systemGraphExecutor = new ContextSwitchingSystemGraphQueryExecutor( databaseManager, getThreadToStatementContextBridge() );
        AssertableLogProvider log = new AssertableLogProvider();
        SecurityLog securityLog = new SecurityLog( log.getLog( getClass() ) );

        realm = TestSystemGraphRealm.testRealm( new ImportOptionsBuilder().build(),
                securityLog, managementService, systemGraphExecutor, Config.defaults() );
    }

    @AfterEach
    void tearDown()
    {
        if ( database != null )
        {
            managementService.shutdown();
            database = null;
        }
    }

    private DatabaseManager<?> getDatabaseManager()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private ThreadToStatementContextBridge getThreadToStatementContextBridge()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    @Test
    void defaultNodes()
    {
        // should have system and default databases
        assertEquals( DB_NODE_COUNT, nbrOfDbNodes() );

        // should have default neo4j user
        assertEquals( USER_NODE_COUNT, nbrOfUserNodes() );

        // should have default roles
        assertEquals( ROLE_NODE_COUNT, nbrOfRoleNodes() );

        // default privileges
        assertEquals( PRIVILEGE_NODE_COUNT, nbrOfPrivilegeNodes() );

        // default privilege assignments
        assertEquals( PRIVILEGE_ASSIGNMENT_COUNT, nbrOfPrivilegeAssignments() );

        // graph, token, schema, system
        assertEquals( RESOURCE_NODE_COUNT, nbrOfResourceNodes() );
    }

    @Test
    void shouldShareRoleNodeBetweenUsersWithSameRole() throws Exception
    {
        setupTwoReaders();
        assertEquals( nbrOfRoleNodes(), ROLE_NODE_COUNT );
        assertEquals( nbrOfUserNodes(), USER_NODE_COUNT + 2 );
    }

    @Test
    void shouldFailShowPrivilegeForUnknownUser()
    {
        assertException( () -> realm.showPrivilegesForUser( "TomRiddle" ), InvalidArgumentsException.class, "User 'TomRiddle' does not exist." );
        assertException( () -> realm.showPrivilegesForUser( "" ), InvalidArgumentsException.class, "User '' does not exist." );
        assertException( () -> realm.showPrivilegesForUser( "Neo," ), InvalidArgumentsException.class, "User 'Neo,' does not exist." );
    }

    @Test
    void shouldAddAlreadyGrantedPrivilegeWithoutDuplication() throws Exception
    {
        debugPrivileges();
        // Given
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newRole( "custom", "Neo" );
        DatabasePrivilege dbPrivilege = new DatabasePrivilege();
        dbPrivilege.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        DatabasePrivilege dbPrivilege2 = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPrivilege2.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
        debugPrivileges( "custom" );

        // When
        realm.grantPrivilegeToRole( "custom", dbPrivilege );
        realm.grantPrivilegeToRole( "custom", dbPrivilege2 );
        debugPrivileges( "custom" );

        // Then
        assertThat( realm.getPrivilegesForRoles( Set.of( "custom" ) ), containsInAnyOrder( dbPrivilege, dbPrivilege2 ) );
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT + 1 ) );
        assertThat( nbrOfPrivilegeAssignments(), equalTo( PRIVILEGE_ASSIGNMENT_COUNT + 2 ) );
        assertThat( nbrOfResourceNodes(), equalTo( RESOURCE_NODE_COUNT ) );

        // When
        realm.grantPrivilegeToRole( "custom", dbPrivilege );
        realm.grantPrivilegeToRole( "custom", dbPrivilege2 );
        debugPrivileges( "custom" );

        // Then
        assertThat( realm.getPrivilegesForRoles( Set.of( "custom" ) ), containsInAnyOrder( dbPrivilege, dbPrivilege2 ) );
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT + 1 ) );
        assertThat( nbrOfPrivilegeAssignments(), equalTo( PRIVILEGE_ASSIGNMENT_COUNT + 2 ) );
        assertThat( nbrOfResourceNodes(), equalTo( RESOURCE_NODE_COUNT ) );

        // When
        realm.revokePrivilegeFromRole( "custom", dbPrivilege );
        debugPrivileges( "custom" );

        // Then
        assertThat( realm.getPrivilegesForRoles( Set.of( "custom" ) ), containsInAnyOrder( dbPrivilege2 ) );
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT + 1 ) );
        assertThat( nbrOfPrivilegeAssignments(), equalTo( PRIVILEGE_ASSIGNMENT_COUNT + 1 ) );
        assertThat( nbrOfResourceNodes(), equalTo( RESOURCE_NODE_COUNT ) );

        // When
        realm.revokePrivilegeFromRole( "custom", dbPrivilege2 );

        // Then
        assertThat( realm.getPrivilegesForRoles( Set.of( "custom" ) ), emptyIterable() );
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT + 1 ) );
        assertThat( nbrOfPrivilegeAssignments(), equalTo( PRIVILEGE_ASSIGNMENT_COUNT ) );
        assertThat( nbrOfResourceNodes(), equalTo( RESOURCE_NODE_COUNT ) );
    }

    @Test
    void shouldCreateNewPrivilegeNodeForDifferentResources() throws Exception
    {
        // Given
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newRole( "custom", "Neo" );
        DatabasePrivilege dbPrivilege1 = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPrivilege1.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
        DatabasePrivilege dbPrivilege2 = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPrivilege2.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SystemResource() ) );

        // When
        realm.grantPrivilegeToRole( "custom", dbPrivilege1 );

        // Then
        assertThat( realm.getPrivilegesForRoles( Set.of( "custom" ) ), containsInAnyOrder( dbPrivilege1 ) );
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT + 1 ) );
        assertThat( nbrOfResourceNodes(), equalTo( RESOURCE_NODE_COUNT ) ); // no new resources added

        // When
        realm.grantPrivilegeToRole( "custom", dbPrivilege2 );

        // Then
        DatabasePrivilege expected = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
        expected.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SystemResource() ) );
        assertThat( realm.getPrivilegesForRoles( Set.of( "custom" ) ), containsInAnyOrder( expected ) );
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT + 2 ) );
        assertThat( nbrOfResourceNodes(), equalTo( RESOURCE_NODE_COUNT ) ); // no new resources added
    }

    @Test
    void shouldCreateNewPrivilegeNodeForDifferentScope() throws Exception
    {
        // Given
        realm.newRole( "custom" );
        DatabasePrivilege dbPrivilege1 = new DatabasePrivilege();
        dbPrivilege1.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        DatabasePrivilege dbPrivilege2 = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPrivilege2.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );

        // When
        realm.grantPrivilegeToRole( "custom", dbPrivilege1 );

        // Then
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT ) );
        assertThat( nbrOfPrivilegeAssignments(), equalTo( PRIVILEGE_ASSIGNMENT_COUNT + 1 ) );

        // When
        realm.grantPrivilegeToRole( "custom", dbPrivilege2 );

        // Then
        assertThat( nbrOfPrivilegeNodes(), equalTo( PRIVILEGE_NODE_COUNT + 1 ) );
        assertThat( nbrOfPrivilegeAssignments(), equalTo( PRIVILEGE_ASSIGNMENT_COUNT + 2 ) );
    }

    @Test
    void shouldRemoveCorrectPrivilege() throws Exception
    {
        // Given
        realm.newRole( "custom" );
        DatabasePrivilege dbPrivilege1 = new DatabasePrivilege();
        dbPrivilege1.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );
        DatabasePrivilege dbPrivilege2 = new DatabasePrivilege( DEFAULT_DATABASE_NAME );
        dbPrivilege2.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );

        realm.grantPrivilegeToRole( "custom", dbPrivilege1 );
        realm.grantPrivilegeToRole( "custom", dbPrivilege2 );

        // When
        realm.revokePrivilegeFromRole( "custom", dbPrivilege1 );
        Set<DatabasePrivilege> privileges = realm.getPrivilegesForRoles( Set.of( "custom" ) );

        // Then
        assertThat( privileges, containsInAnyOrder( dbPrivilege2 ) );
    }

    @Test
    void shouldKeepFullSegmentWhenAddingSmaller() throws Exception
    {
        // Given
        realm.newRole( "custom" );
        DatabasePrivilege dbPriv1 = new DatabasePrivilege();
        dbPriv1.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), Segment.ALL ) );
        DatabasePrivilege dbPriv2 = new DatabasePrivilege();
        dbPriv2.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( "A" ) ) );

        // When
        realm.grantPrivilegeToRole( "custom", dbPriv1 );
        realm.grantPrivilegeToRole( "custom", dbPriv2 );

        // Then
        DatabasePrivilege expected = new DatabasePrivilege();
        expected.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( "A" ) ) );
        expected.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), Segment.ALL ) );
        assertThat( realm.getPrivilegesForRoles( Set.of( "custom" ) ), containsInAnyOrder( expected ) );
    }

    private void setupTwoReaders() throws InvalidArgumentsException
    {
        realm.newUser( "Neo", password( "abc" ), false );
        realm.newUser( "Trinity", password( "123" ), false );
        realm.addRoleToUser( READER, "Neo" );
        realm.addRoleToUser( READER, "Trinity" );
    }

    //TODO: Remove when finished refactoring
    private void debugPrivileges()
    {
        debugPrivileges( "*" );
    }

    private void debugPrivileges( String roleName )
    {
        System.out.println( "Privileges for '" + roleName + "'" );
        systemGraphExecutor.executeQuery(
                "MATCH (r:Role" + (!roleName.equals( "*" ) ? " {name: $role}" : "") + ")-[g]->(a:Action)-[:APPLIES_TO]->(res:Resource), " +
                        "(a)-[:SCOPE]->(s:Segment)-[f:FOR]->(d), (s)-[x:QUALIFIED]->(q) " +
                        "RETURN type(g), r.name, res.type, a.action, id(a), type(f), d.name, type(x), q.label " +
                        "ORDER BY r.name, res.type, a.action, type(f)",
                map( "role", roleName ),
                row ->
                {
                    String text = Arrays.stream( row.fields() ).map( this::valueToString ).collect(
                            Collectors.joining( ", " ) );
                    System.out.println( "\t" + text );
                    return true;
                } );
    }

    private String valueToString( AnyValue value )
    {
        if ( value instanceof org.neo4j.values.storable.Value )
        {
            Object obj = ((org.neo4j.values.storable.Value) value).asObject();
            return (obj == null) ? value.toString() : obj.toString();
        }
        else
        {
            return value.toString();
        }
    }

    private long nbrOfPrivilegeNodes()
    {
        String query = "MATCH (p:Action) RETURN count(p)";
        return systemGraphExecutor.executeQueryLong( query );

    }

    private long nbrOfPrivilegeAssignments()
    {
        String query = "MATCH (r:Role)-->(p:Action) RETURN count(*)";
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
