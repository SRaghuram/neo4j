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

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.junit.jupiter.api.Assertions.assertThrows;
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
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "Node" ) ); // specific node label privilege
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( adminSubject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( adminSubject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, 4 ) );
        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
        assertSuccess( subject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( subject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        // TODO count store not fixed
        //assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
    }

    @Test
    void shouldShowAllNodesForReadAllLabels() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( subject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( subject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, 4 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsForPattern() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "A" ) );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "B" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE (n:A {number: 0}) MERGE (n)-[:R]->(:B {number: 1}) MERGE (n)-[:R]->(:C {number: 2})" );
        assertEmpty( adminSubject, "CREATE (n:A {number: 3})" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( adminSubject, "MATCH ()-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1, 2 ) );
        assertSuccess( adminSubject, "MATCH (:A)-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1, 2 ) );
        assertSuccess( adminSubject, "MATCH p = ()-[:R]->() return count(p)", r -> assertKeyIs( r, "count(p)", 2 ) );
        assertSuccess( subject, "MATCH ()-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1 ) );
        assertSuccess( subject, "MATCH (:A)-[:R]->(n) return n.number", r -> assertKeyIs( r, "n.number", 1 ) );
        assertSuccess( subject, "MATCH p = ()-[:R]->() return count(p)", r -> assertKeyIs( r, "count(p)", 1 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsForVarLengthPattern() throws Throwable
    {
        // (a:A)-[:R]->(:B)-[:R]->(:B)<-[:R]-(:C)<-[:R]-(:D)<-[:R]-(a)
        assertEmpty( adminSubject,
                "CREATE (n:A {number: 0}) " +
                        "MERGE (n)-[:R]->(:B {number: 1})-[:R]->(b:B {number: 2}) " +
                        "MERGE (n)-[:R]->(:C {number: 3})-[:R]->(:D {number: 4})-[:R]->(b)" );

        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege(new ResourcePrivilege( "read", "label", "A" ) );
        dbPriv.addPrivilege(new ResourcePrivilege( "read", "label", "B" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        CommercialLoginContext subject = neo.login( "Alice", "foo" );

        assertSuccess( adminSubject, "MATCH (:A)-[:R*3]->(n:B) return n.number", r -> assertKeyIs( r, "n.number", 2 ) );
        assertSuccess( adminSubject, "MATCH (:A)-[:R*..3]->(n:B) return n.number", r -> assertKeyIs( r, "n.number", 1, 2, 2 ) );
        assertSuccess( adminSubject, "MATCH p = (:A)-[:R*..3]->(n:B) return count(p)", r -> assertKeyIs( r, "count(p)", 3 ) );
        assertSuccess( subject, "MATCH (:A)-[:R*3]->(n:B) return n.number", r -> assertKeyIs( r, null ) );
        assertSuccess( subject, "MATCH (:A)-[:R*..3]->(n:B) return n.number", r -> assertKeyIs( r, "n.number", 1, 2 ) );
        assertSuccess( subject, "MATCH p = (:A)-[:R*..3]->(n:B) return count(p)", r -> assertKeyIs( r, "count(p)", 2 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsForShortestPath() throws Throwable
    {
        // (a:A)-[:R]->(:B)-[:R]->(:B)-[:R]->(:C)<-[:R]-(:D)<-[:R]-(a)
        assertEmpty( adminSubject,
                "CREATE (a:A) " +
                        "MERGE (a)-[:R]->(:B)-[:R]->(:B)-[:R]->(c:C) " +
                        "MERGE (a)-[:R]->(:D)-[:R]->(c)" );

        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );

        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "A" ) );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "B" ) );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "C" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        CommercialLoginContext subject = neo.login( "Alice", "foo" );

        assertSuccess( adminSubject, "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) return length(p) as length", r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( subject, "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) return length(p) as length", r -> assertKeyIs( r, "length", 3 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedLabelsForNode() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "Node" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( adminSubject, "MATCH (n:Node) WHERE n.number = 3 return labels(n)",
                r -> assertKeyIs( r, "labels(n)", listOf( "A", "Node" ) ) );
        assertSuccess( subject, "MATCH (n:Node) WHERE n.number = 3 return labels(n)",
                r -> assertKeyIs( r, "labels(n)", listOf( "Node" ) ) );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithIndex() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "Node" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE INDEX ON :A(number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number" );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithIndexSeek() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "Node" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE INDEX ON :A(number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number > 0 return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number > 0 return n.number" );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithConstraint() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "Node" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT a.number IS UNIQUE" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number" );
    }

    @Test
    void shouldOnlyShowWhitelistedNodeLabelsWithExistConstraint() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "read", "label", "Node" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertEmpty( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number" );
    }

    @Test
    void shouldRespectConstraintsWithoutReadPrivileges() throws Throwable
    {
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        DatabasePrivilege dbPriv = new DatabasePrivilege( "*" );
        dbPriv.addPrivilege( new ResourcePrivilege( "write", "graph" ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );
//        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.number)" );
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT a.number IS UNIQUE" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertSuccess( subject, "CREATE (:A)", ResourceIterator::close );
        assertSuccess( adminSubject, "MATCH (a:A) RETURN a.number",
                r -> assertKeyIs( r, "a.number", 4, null ));
    }

    // TESTS
    // add privilege on two labels, remove one, should still have the other one

}
