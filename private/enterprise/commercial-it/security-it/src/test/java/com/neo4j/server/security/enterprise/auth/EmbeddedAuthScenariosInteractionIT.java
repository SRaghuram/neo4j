/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.CommercialLoginContext;
import com.neo4j.server.security.enterprise.auth.ResourcePrivilege.Action;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
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
    void shouldAllowFindForCustomRoleWithFindPrivilege() throws Throwable
    {
        // Given
        userManager.newUser( "Alice", password( "foo" ), false );
        userManager.newRole( "custom", "Alice" );

        // When
        CommercialLoginContext subject = neo.login( "Alice", "foo" );

        // Then
        testFailRead( subject, 3 );

        // When
        DatabasePrivilege dbPriv = new DatabasePrivilege();
        dbPriv.addPrivilege( new ResourcePrivilege( Action.FIND, new Resource.GraphResource() ) );
        userManager.grantPrivilegeToRole( "custom", dbPriv );

        // Then
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
        DatabasePrivilege dbPriv = new DatabasePrivilege();
        dbPriv.addPrivilege( new ResourcePrivilege( Action.FIND, new Resource.GraphResource() ) );
        dbPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
        userManager.grantPrivilegeToRole( roleName, dbPriv );

        // Then
        testSuccessfulRead( subject, 3 );
        testSuccessfulWrite( subject );

        // When
        dbPriv = new DatabasePrivilege();
        dbPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() ) );
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
        DatabasePrivilege dbPriv = new DatabasePrivilege();
        dbPriv.addPrivilege( new ResourcePrivilege( Action.WRITE, new Resource.SystemResource() ) );
        userManager.grantPrivilegeToRole( "UserManager", dbPriv );

        // Then
        CommercialLoginContext subject = neo.login( "Alice", "foo" );
        assertEmpty( subject, "CALL dbms.security.createUser('Bob', 'bar', false)" );
        testFailRead( subject, 3 );
    }

    @Test
    void shouldAllowChangingBuiltinRoles() throws InvalidArgumentsException
    {
        DatabasePrivilege privilege = new DatabasePrivilege();
        privilege.addPrivilege( new ResourcePrivilege( Action.READ, new Resource.GraphResource() ) );

        for ( String role : Arrays.asList( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER, PredefinedRoles.EDITOR,
                PredefinedRoles.READER ) )
        {
            userManager.revokePrivilegeFromRole( role, privilege );
            userManager.grantPrivilegeToRole( role, privilege );
        }
    }

    // Read properties test

    @Test
    void shouldOnlyShowWhitelistedProperties() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "Node" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( adminSubject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, 4 ) );
        assertSuccess( adminSubject, "MATCH (n:A) return n.number", r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
        assertSuccess( subject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( subject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, null ) );
        assertSuccess( subject, "MATCH (n:A) return n.number", r -> assertKeyIs( r, "n.number", null, 3 ) );
    }

    @Test
    void shouldShowAllNodesForReadAllLabels() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), Segment.ALL )
        );

        assertSuccess( subject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( subject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, 4 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
    }

    @Test
    void shouldOnlyAllowReadOnWhitelistedProperties() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "B" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "C" ) ) )
        );

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
    void shouldOnlyShowWhitelistedPropertiesWithIndex() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        CommercialLoginContext readOnly = setupUserAndLogin(
                "readOnlyUser", "readOnlyRole",
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "Node" ) ) )
        );
        CommercialLoginContext findAndRead = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "Node" ) ) )
        );

        String query = "MATCH (n:A) WHERE n.number IS NOT NULL return n.number";
        String queryIndex = "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number";

        assertSuccess( adminSubject, query, r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( findAndRead, query, r -> assertKeyIs( r, "n.number", 3 ) );
        assertEmpty( readOnly, query );

        assertEmpty( adminSubject, "CREATE INDEX ON :A(number)" );
        assertEmpty( adminSubject, "CALL db.awaitIndexes" );

        assertSuccess( adminSubject, queryIndex, r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( findAndRead, queryIndex, r -> assertKeyIs( r, "n.number", 3 ) );
        assertEmpty( readOnly, queryIndex );
    }

    @Test
    void shouldOnlyShowWhitelistedPropertiesWithIndexSeek() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE INDEX ON :A(number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "Node" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number > 0 return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number > 0 return n.number", r -> assertKeyIs( r, "n.number", 3 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedPropertiesWithConstraint() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT a.number IS UNIQUE" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "Node" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( subject, "MATCH (n:A) WHERE n.number IS NOT NULL return n.number", r -> assertKeyIs( r, "n.number", 3 ) );
        assertSuccess( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number", r -> assertKeyIs( r, "n.number", 3 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedPropertiesWithExistConstraint() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );

        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "Node" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( subject, "MATCH (n:A) WHERE n.number IS NOT NULL return n.number", r -> assertKeyIs( r, "n.number", 3 ) );
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.number)" );
        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number", r -> assertKeyIs( r, "n.number", 3 ) );
    }

    @Test
    void shouldRespectExistsConstraintsWithoutReadPrivileges() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT exists(a.number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() )
        );

        assertFail( adminSubject, "CREATE (:A)", "with label `A` must have the property `number`" );
        assertFail( subject, "CREATE (:A)", "with label `A` must have the property `number`" );
        assertSuccess( adminSubject, "MATCH (a:A) RETURN count(a)", r -> assertKeyIs( r, "count(a)", 1 ));
    }

    @Test
    void shouldRespectUniqueConstraintsWithoutReadPrivileges() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT a.number IS UNIQUE" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() )
        );

        assertFail( adminSubject, "CREATE (:A {number: 4})", "already exists with label `A` and property `number` = 4" );
        assertFail( subject, "CREATE (:A {number: 4})", "already exists with label `A` and property `number` = 4" );
        assertSuccess( adminSubject, "MATCH (a:A) RETURN count(a)", r -> assertKeyIs( r, "count(a)", 1 ));
    }

    @Test
    void shouldOnlyShowPropertiesOnWhitelistedNodeLabelsForPattern() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A {number: 0}) MERGE (n)-[:R]->(:B {number: 1}) MERGE (n)-[:R]->(:C {number: 2})" );
        assertEmpty( adminSubject, "CREATE (n:A {number: 3})" );
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "B" ) ) ) );

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
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "B" ) ) ) );

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
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "B" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "C" ) ) ) );

        String shortestBasic = "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) return length(p) as length";
        String shortestWithProperty =
                "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) " +
                "WHERE ALL (n in nodes(p) WHERE exists(n.number)) return length(p) as length";

        assertSuccess( adminSubject, shortestBasic, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( adminSubject, shortestWithProperty, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( subject, shortestBasic, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( subject, shortestWithProperty, r -> assertKeyIs( r, "length", 3 ) );
    }

    @Test
    void shouldReadBackNodeCreatedInSameTransaction() throws Throwable
    {
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() )
        );

        assertEmpty( adminSubject, "CREATE (:A {foo: 1})" );

        String query = "CREATE (:A {foo: $param}) WITH 1 as bar MATCH (a:A) RETURN a.foo";
        assertSuccess( adminSubject, query, Collections.singletonMap("param", 2L), r -> assertKeyIs( r, "a.foo", 1, 2 ) );
        assertSuccess( subject, query, Collections.singletonMap("param", 3L), r -> assertKeyIs( r, "a.foo", null, null, 3 ) );
    }

    @Test
    void shouldReadBackIndexedNodeCreatedInSameTransaction() throws Throwable
    {
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.WRITE, new Resource.GraphResource() )

        );

        setupGraph();
        assertEmpty( adminSubject, "CREATE (:A {foo: 1})" );

        assertEmpty( adminSubject, "CREATE INDEX ON :A(foo)" );
        assertEmpty( adminSubject, "CALL db.awaitIndexes" );

        String query = "CREATE (:A {foo: $param}) WITH 1 as bar MATCH (a:A) USING INDEX a:A(foo) WHERE EXISTS(a.foo) RETURN a.foo";
        assertSuccess( adminSubject, query, Collections.singletonMap("param", 2L), r -> assertKeyIs( r, "a.foo", 1, 2 ) );
        assertSuccess( subject, query, Collections.singletonMap("param", 3L), r -> assertKeyIs( r, "a.foo", 3 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedPropertiesForSegment() throws Throwable
    {
        CommercialLoginContext findSome = setupUserAndLogin(
                "user1", "role1",
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.PropertyResource( "foo" ), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.PropertyResource( "bar" ), new Segment( Collections.singleton( "B" ) ) )
        );

        CommercialLoginContext findAll = setupUserAndLogin(
                "user2", "role2",
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.READ, new Resource.PropertyResource( "foo" ), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.PropertyResource( "bar" ), new Segment( Collections.singleton( "B" ) ) )
        );

        assertEmpty( adminSubject, "MATCH (n) DETACH DELETE n" );
        assertEmpty( adminSubject, "CREATE (:A {foo: 1, bar: 2}), (:B {foo: 3, bar: 4}), (:A:B {foo: 5, bar: 6}), ({foo: 7, bar: 8})" );

        assertSuccess( adminSubject, "MATCH (n) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, 3, 5, 7 ) );
        assertSuccess( findAll, "MATCH (n) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, null, 5, null ) );
        assertSuccess( findSome, "MATCH (n) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, 5 ) );

        assertSuccess( adminSubject, "MATCH (n:A) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, 5 ) );
        assertSuccess( findAll, "MATCH (n:A) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 5 ) );
        assertSuccess( findSome, "MATCH (n:A) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 5 ) );

        assertEmpty( adminSubject, "CREATE INDEX ON :A(foo, bar)" );
        assertEmpty( adminSubject, "CALL db.awaitIndexes" );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(foo, bar) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo",
                r -> assertKeyIs( r, "n.foo", 1, 5 ) );
        assertSuccess( findAll, "MATCH (n:A) USING INDEX n:A(foo, bar) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo",
                r -> assertKeyIs( r, "n.foo", 5 ) );
        assertSuccess( findSome, "MATCH (n:A) USING INDEX n:A(foo, bar) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo",
                r -> assertKeyIs( r, "n.foo", 5 ) );
    }

    // FIND privilege tests

    @Test
    void shouldFindWhitelistedNodes() throws Throwable
    {
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) )
        );

        assertEmpty( adminSubject, "MATCH (n) DETACH DELETE n" );
        assertEmpty( adminSubject, "CREATE (:A), (:B), (:A:B), ()" );

        assertSuccess( subject, "MATCH (n) RETURN labels(n)",
                r -> assertKeyIs( r, "labels(n)", Collections.singletonList( "A" ), List.of( "A", "B" ) ) );
    }

    @Test
    void shouldGetCountForWhitelistedNodes() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A)" );
        assertEmpty( adminSubject, "CREATE (n:Node:A)" );
        CommercialLoginContext subject =
                setupUserAndLogin( new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 2 ) );
        assertSuccess( adminSubject, "MATCH (n:A) return count(n)", r -> assertKeyIs( r, "count(n)", 2 ) );
        assertSuccess( subject, "MATCH (n:A) return count(n)", r -> assertKeyIs( r, "count(n)", 2 ) );
        assertSuccess( adminSubject, "MATCH (n:Node) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
        assertSuccess( subject, "MATCH (n:Node) return count(n)", r -> assertKeyIs( r, "count(n)", 1 ) );
    }

    @Test
    void shouldOnlyShowLabelsForScope() throws Throwable
    {
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) )
        );

        assertEmpty( adminSubject, "MATCH (n) DETACH DELETE n" );
        assertEmpty( adminSubject, "CREATE (:A), (:B), (:A:B), ()" );

        assertSuccess( subject, "MATCH (n) RETURN labels(n)",
                r -> assertKeyIs( r, "labels(n)", Collections.singletonList( "A" ), List.of( "A", "B" ) ) );
    }

    @Test
    void shouldNotTraverseExplicitLabelsWithoutPermission() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "D" ) ) ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "C" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (a:A)--(n1:D) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( subject, "MATCH (a:A)--(n1:D) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( adminSubject, "MATCH (a:A)--(n1:D)--(c:C) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( subject, "MATCH (a:A)--(n1:D)--(c:C) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( adminSubject, "MATCH (a:A)--(n1:D)--(c:C)--(b:B) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( subject, "MATCH (a:A)--(n1:D)--(c:C)--(b:B) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 0 ) );
    }

    @Test
    void shouldNotTraverseWithOnlyReadPrivileges() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "D" ) ) ),
                new ResourcePrivilege( Action.READ, new Resource.AllPropertiesResource(), new Segment( Collections.singleton( "C" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (a:A)--(n1:D)--(c:C) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( subject, "MATCH (a:A)--(n1:D)--(c:C) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 0 ) );
        assertSuccess( adminSubject, "MATCH (a:A)--(n1:D)--(c:C)--(b:B) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( subject, "MATCH (a:A)--(n1:D)--(c:C)--(b:B) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 0 ) );
    }

    @Test
    void shouldOnlyFindWhitelistedNodeLabelsForVarLengthPattern() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "B" ) ) ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "C" ) ) )
        );

        String varlenBasic = "MATCH p = (:A)-[:R*3]-(n:C) return length(p)";
        String varlenMinMax = "MATCH p = (:A)-[:R*..3]-(n:C) return length(p)";
        String varlenPredicate = "MATCH p = (:A)-[:R*..3]-(n:C) WHERE ALL (n IN nodes(p) WHERE exists(n.number)) return length(p)";

        assertSuccess( adminSubject, varlenBasic, r -> assertKeyIs( r, "length(p)", 3 ) );
        assertSuccess( adminSubject, varlenMinMax, r -> assertKeyIs( r, "length(p)", 2, 3 ) );
        assertSuccess( adminSubject, varlenPredicate, r -> assertKeyIs( r, "length(p)", 2, 3 ) );
        assertSuccess( subject, varlenBasic, r -> assertKeyIs( r, "length(p)", 3 ) );
        assertSuccess( subject, varlenMinMax, r -> assertKeyIs( r, "length(p)", 3 ) );
        assertEmpty( subject, varlenPredicate );
    }

    @Test
    void shouldOnlyFindWhitelistedNodeLabelsForShortestPath() throws Throwable
    {
        setupGraph();
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "B" ) ) ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "C" ) ) )
        );

        String shortestBasic = "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) return length(p) as length";
        String shortestWithProperty =
                "MATCH (a:A), (c:C), p = shortestPath((a)-[:R*]-(c)) " +
                        "WHERE ALL (n in nodes(p) WHERE exists(n.number)) return length(p) as length";

        assertSuccess( adminSubject, shortestBasic, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( adminSubject, shortestWithProperty, r -> assertKeyIs( r, "length", 2 ) );
        assertSuccess( subject, shortestBasic, r -> assertKeyIs( r, "length", 3 ) );
        assertEmpty( subject, shortestWithProperty );
    }

    @Test
    void shouldKeepFullSegmentWhenAddingRestricted() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A)" );

        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) )
        );

        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
    }

    @Test
    void shouldRemoveCorrectPrivilegeForDifferentSegments() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A)" );

        CommercialLoginContext subject = setupUserAndLogin(
                "Alice", "custom",
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ),
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), new Segment( Collections.singleton( "A" ) ) )
        );

        DatabasePrivilege privilege = new DatabasePrivilege();
        privilege.addPrivilege( new ResourcePrivilege( Action.FIND, new Resource.GraphResource(), Segment.ALL ) );

        userManager.revokePrivilegeFromRole( "custom", privilege );

        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 1 ) );
    }

    @Test
    void shouldReadPropertiesOnRelationship() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (:A)-[:REL {foo: 1}]->(:B)" );
        CommercialLoginContext subject = setupUserAndLogin(
                new ResourcePrivilege( Action.FIND, new Resource.GraphResource() )
        );

        String query = "MATCH (:A)-[r]->(:B) RETURN r.foo";

        assertSuccess( adminSubject, query, r -> assertKeyIs( r, "r.foo", 1 ) );
        assertSuccess( subject, query, r -> assertKeyIs( r, "r.foo", 1 ) );
    }

    // Helpers

    private CommercialLoginContext setupUserAndLogin( ResourcePrivilege... privileges ) throws Exception
    {
        return setupUserAndLogin( "Alice", "custom", privileges );
    }

    private CommercialLoginContext setupUserAndLogin( String username, String roleName, ResourcePrivilege... privileges ) throws Exception
    {
        DatabasePrivilege dbPriv = new DatabasePrivilege();
        for ( ResourcePrivilege privilege : privileges )
        {
            dbPriv.addPrivilege( privilege );
        }
        userManager.newUser( username, password( "foo" ), false );
        userManager.newRole( roleName, username );
        userManager.grantPrivilegeToRole( roleName, dbPriv );
        return neo.login( username, "foo" );
    }

    private void setupGraph()
    {
        // (a:A)-[:R]->(:B)-[:R]->(:B)-[:R]->(:C)<-[:R]-(:D)<-[:R]-(a)
        assertEmpty( adminSubject,
                "CREATE (a:A {number: 0}) " +
                        "MERGE (a)-[:R]->(:B {number: 1})-[:R]->(b:B {number: 2})-[:R]-(c:C {number: 3}) " +
                        "MERGE (a)-[:R]->(:D {number: 4})-[:R]->(c)" );
    }
}
