/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.server.security.auth.SecurityTestUtils.password;

public class EmbeddedAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<CommercialLoginContext>
{
    @Override
    protected NeoInteractionLevel<CommercialLoginContext> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new EmbeddedInteraction( config, testDirectory );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Integer )
        {
            return ((Integer) obj).longValue();
        }
        else
        {
            return obj;
        }
    }

    @Test
    void shouldAllowReadsForCustomRoleWithReadPrivilege() throws Throwable
    {
        // Given
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "CustomRead", "Alice" );

        // When
        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "graph" ) );
        userManager.grantPrivilegeToRole( "CustomRead", dbPriv );

        // Then
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        testSuccessfulRead( subject, 3 );
        testFailWrite( subject );
    }

    @Test
    void shouldRevokePrivilegeFromRole() throws Throwable
    {
        // Given
        String roleName = "CustomRole";
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( roleName, "Alice" );

        // When
        CommercialLoginContext subject = neo.login( "Alice", "foo" );

        // Then
        testFailRead( subject, 3 );
        testFailWrite( subject );

        // When
        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "graph" ) );
        dbPriv.addPrivilege( new ResourcePrivilege( "write", "graph" ) );
        userManager.grantPrivilegeToRole( roleName, dbPriv );

        // Then
        testSuccessfulRead( subject, 3 );
        testSuccessfulWrite( subject );

        // When
        dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "write", "graph" ) );
        userManager.revokePrivilegeFromRole( roleName, dbPriv );

        // Then
        testSuccessfulRead( subject, 4 );
        testFailWrite( subject );
    }

    @Test
    void shouldAllowUserManagementForCustomRoleWithAdminPrivilege() throws Throwable
    {
        // Given
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "UserManager", "Alice" );

        // When
        userManager.setAdmin( "UserManager", true );

        // Then
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertEmpty( subject, "CALL dbms.security.createUser('Bob', 'bar', false)" );
        testFailRead( subject, 3 );
    }

    @Test
    void shouldNotAllowChangingBuiltinRoles() throws InvalidArgumentsException
    {
        DatabasePrivilege privilege = new DatabasePrivilege( "*" );
        privilege.addPrivilege( new ResourcePrivilege( "read", "graph" ) );

        for ( String role : Arrays.asList( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER, PredefinedRoles.EDITOR,
                PredefinedRoles.READER ) )
        {
            assertThrows( InvalidArgumentsException.class, () -> userManager.setAdmin( role, true ) );
            assertThrows( InvalidArgumentsException.class, () -> userManager.setAdmin( role, false ) );
            assertThrows( InvalidArgumentsException.class, () -> userManager.revokePrivilegeFromRole( role, privilege ) );
            assertThrows( InvalidArgumentsException.class, () -> userManager.grantPrivilegeToRole( role, privilege ) );
        }
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabels() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label", "Node" ) );

        assertSuccess( adminSubject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( adminSubject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, 4 ) );
        assertSuccess( adminSubject, "MATCH (n:A) return n.number", r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
        assertSuccess( subject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( subject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, null ) );
        assertEmpty( subject, "MATCH (n:A) return n.number" );
    }

    @Test
    void shouldGetCountForWhitelistedNodes() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A)" );
        assertEmpty( adminSubject, "CREATE (n:Node:A)" );
        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label", "Node" ) );

        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
        assertSuccess( adminSubject, "MATCH (n:A) return count(n)", r -> assertKeyIs( r, "count(n)", 2 ) );
        assertSuccess( subject, "MATCH (n:A) return count(n)", r -> assertKeyIs( r, "count(n)", 0 ) );
    }

    @Test
    void shouldShowAllNodesForReadAllLabels() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label" ) );

        assertSuccess( subject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( subject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, 4 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
    }

    @Test
    void shouldOnlyAllowReadOnWhitelistedLabels() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( "read", "label", "A" ),
                new ResourcePrivilege( "read", "label", "B" ),
                new ResourcePrivilege( "read", "label", "C" ) );

        assertSuccess( adminSubject, "MATCH (a:A)-[]->(n)-[]->(c:C) RETURN a.number AS a, n.number AS n, c.number AS c", r ->
        {
            assertTrue( r.hasNext() );
            Map<String,Object> row = r.next();
            assertThat( row.get( "a" ), equalTo( 0L ) );
            assertThat( row.get( "n" ), equalTo( 4L ) );
            assertThat( row.get( "c" ), equalTo( 3L ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        assertSuccess( subject, "MATCH (a:A)-[]->(n)-[]->(c:C) RETURN a.number AS a, n.number AS n, c.number AS c", r ->
        {
            assertTrue( r.hasNext() );
            Map<String,Object> row = r.next();
            assertThat( row.get( "a" ), equalTo( 0L ) );
            assertNull( row.get( "n" ) );
            assertThat( row.get( "c" ), equalTo( 3L ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );

        assertSuccess( subject, "MATCH (a:A)-[]->(n1)-[]->(n2)-[]->(c:C) RETURN a.number AS a, n1.number AS n1, n2.number AS n2, c.number AS c", r ->
        {
            assertTrue( r.hasNext() );
            Map<String,Object> row = r.next();
            assertThat( row.get( "a" ), equalTo( 0L ) );
            assertThat( row.get( "n1" ), equalTo( 1L ) );
            assertThat( row.get( "n2" ), equalTo( 2L ) );
            assertThat( row.get( "c" ), equalTo( 3L ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );
    }

    @Test
    void shouldNotTraverseExplicitLabelsWithoutPermission() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( "read", "label", "A" ),
                new ResourcePrivilege( "read", "label", "B" ) );

        assertSuccess( adminSubject, "MATCH (a:A)-[]->(n1:D)-[]->(c:C) RETURN a.number AS a, n1.number AS n1, c.number AS c", r ->
        {
            assertTrue( r.hasNext() );
            Map<String,Object> row = r.next();
            assertThat( row.get( "a" ), equalTo( 0L ) );
            assertThat( row.get( "n1" ), equalTo( 4L ) );
            assertThat( row.get( "c" ), equalTo( 3L ) );
            assertThat( r.hasNext(), equalTo( false ) );
        } );
        assertEmpty( subject, "MATCH (a:A)-[]->(n1:C)-[]->(n2:D)-[]->(b:B) RETURN a.number AS a, n1.number AS n1, n2.number AS n2, b.number AS b" );
    }

    @Test
    void shouldOnlyShowWhitelistedLabelsForNode() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label", "Node" ) );

        assertSuccess( adminSubject, "MATCH (n:Node) WHERE n.number = 3 return labels(n)",
                r -> assertKeyIs( r, "labels(n)", listOf( "A", "Node" ) ) );
        assertSuccess( subject, "MATCH (n:Node) WHERE n.number = 3 return labels(n)",
                r -> assertKeyIs( r, "labels(n)", listOf( "Node" ) ) );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithIndex() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE INDEX ON :A(number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label", "Node" ) );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number" );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithIndexSeek() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE INDEX ON :A(number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label", "Node" ) );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number > 0 return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number > 0 return n.number" );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithConstraint() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT a.number IS UNIQUE" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label", "Node" ) );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) WHERE n.number IS NOT NULL return n.number" );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number" );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithExistConstraint() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "read", "label", "Node" ) );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) WHERE n.number IS NOT NULL return n.number" );
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.number)" );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number" );
    }

    @Test
    void shouldRespectExistsConstraintsWithoutReadPrivileges() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "write", "graph" ) );

        assertFail( adminSubject, "CREATE (:A)", "with label `A` must have the property `number`" );
        assertFail( subject, "CREATE (:A)", "with label `A` must have the property `number`" );
        assertSuccess( adminSubject, "MATCH (a:A) RETURN count(a)", r -> assertKeyIs( r, "count(a)", 1 ));
    }

    @Test
    void shouldRespectUniqueConstraintsWithoutReadPrivileges() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT a.number IS UNIQUE" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        CommercialLoginContext subject = setupUserAndLogin( new ResourcePrivilege( "write", "graph" ) );

        assertFail( adminSubject, "CREATE (:A {number: 4})", "already exists with label `A` and property `number` = 4" );
        assertFail( subject, "CREATE (:A {number: 4})", "already exists with label `A` and property `number` = 4" );
        assertSuccess( adminSubject, "MATCH (a:A) RETURN count(a)", r -> assertKeyIs( r, "count(a)", 1 ));
    }

    private CommercialLoginContext setupUserAndLogin( ResourcePrivilege... privileges ) throws Exception
    {
        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        for ( ResourcePrivilege privilege : privileges )
        {
            dbPriv.addPrivilege( privilege );
        }
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );
        userManager.grantPrivilegeToRole( "custom", dbPriv );
        return neo.login( "Alice", "foo" );
    }

    private void setupGraph()
    {
        // (a:A)-[:R]->(:B)-[:R]->(:B)-[:R]->(:C)<-[:R]-(:D)<-[:R]-(a)
        assertEmpty( adminSubject,
                "CREATE (a:A {number: 0}) " +
                        "MERGE (a)-[:R]->(:B {number: 1})-[:R]->(b:B {number: 2})-[:R]-(c:C {number: 3}) " +
                        "MERGE (a)-[:R]->(:D {number: 4})-[:R]->(c)" );
    }

    @Test
    void shouldOnlyShowPropertiesOnWhitelistedNodeLabelsForPattern() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A {number: 0}) MERGE (n)-[:R]->(:B {number: 1}) MERGE (n)-[:R]->(:C {number: 2})" );
        assertEmpty( adminSubject, "CREATE (n:A {number: 3})" );
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( "read", "label", "A" ),
                new ResourcePrivilege( "read", "label", "B" ) );

        assertSuccess( adminSubject, "MATCH ()-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1, 2 ) );
        assertSuccess( adminSubject, "MATCH (:A)-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1, 2 ) );
        assertSuccess( adminSubject, "MATCH p = ()-[:R]->() return count(p)", r -> assertKeyIs( r, "count(p)", 2 ) );
        assertSuccess( subject, "MATCH ()-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1, null ) );
        assertSuccess( subject, "MATCH (:A)-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1, null ) );
        assertSuccess( subject, "MATCH p = ()-[:R]->() return count(p)", r -> assertKeyIs( r, "count(p)", 2 ) );
    }

    @Test
    void shouldOnlyReadPropertiesWhitelistedNodeLabelsForVarLengthPattern() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( "read", "label", "A" ),
                new ResourcePrivilege( "read", "label", "B" ) );

        String varlenBasic = "MATCH (:A)-[:R*3]-(n:B) return n.number";
        String varlenMinMax = "MATCH (:A)-[:R*..3]-(n:B) return n.number";
        String varlenPredicate = "MATCH p = (:A)-[:R*..3]-(n:B) WHERE ALL (n IN nodes(p) WHERE exists(n.number)) return n.number";

        assertSuccess( adminSubject, varlenBasic, r -> assertKeyIs( r, "n.number", 2 ) );
        assertSuccess( adminSubject, varlenMinMax, r -> assertKeyIs( r, "n.number", 1, 2, 2 ) );
        assertSuccess( adminSubject, varlenPredicate, r -> assertKeyIs( r, "n.number", 1, 2, 2 ) );
        assertSuccess( subject, varlenBasic, r -> assertKeyIs( r, "n.number", 2 ) );
        assertSuccess( subject, varlenMinMax, r -> assertKeyIs( r, "n.number", 1, 2, 2 ) );
        assertSuccess( subject, varlenPredicate, r -> assertKeyIs( r, "n.number", 1, 2 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsForShortestPath() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( "read", "label", "A" ),
                new ResourcePrivilege( "read", "label", "B" ),
                new ResourcePrivilege( "read", "label", "C" ) );

        String shortestBasic = "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) return length(p) as length";
        String shortestWithProperty =
                "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) " +
                "WHERE ALL (n in nodes(p) WHERE exists(n.number)) return length(p) as length";

        assertSuccess( adminSubject, shortestBasic, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( adminSubject, shortestWithProperty, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( subject, shortestBasic, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( subject, shortestWithProperty, r -> assertKeyIs( r, "length", 3 ) );
    }
}
