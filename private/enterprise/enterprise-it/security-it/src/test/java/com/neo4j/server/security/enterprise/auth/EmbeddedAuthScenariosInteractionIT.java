/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import com.neo4j.kernel.enterprise.api.security.EnterpriseLoginContext;
import com.neo4j.server.security.enterprise.auth.plugin.api.PredefinedRoles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class EmbeddedAuthScenariosInteractionIT extends AuthScenariosInteractionTestBase<EnterpriseLoginContext>
{
    private static final String PASSWORD = "foo";

    @Override
    protected NeoInteractionLevel<EnterpriseLoginContext> setUpNeoServer( Map<Setting<?>,String> config, TestInfo testInfo )
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
        createUserWithRole( "Alice", "custom" );

        // When
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        // Then
        testFailRead( subject, ACCESS_DENIED );

        // When
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", "custom" ) );
            transaction.execute( "GRANT TRAVERSE ON GRAPH * TO custom" );
            transaction.commit();
        }

        // Then
        testSuccessfulRead( subject, 3 );
        testFailWrite( subject );
    }

    @Test
    void shouldRevokePrivilegeFromRole() throws Throwable
    {
        // Given
        String roleName = "CustomRole";
        assertDDLCommandSuccess( adminSubject, String.format( "CREATE USER Alice SET PASSWORD '%s' CHANGE NOT REQUIRED", PASSWORD ) );
        createRole( roleName, "Alice" );

        // When
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        // Then
        testFailRead( subject, ACCESS_DENIED );
        testFailWrite( subject, ACCESS_DENIED );

        // When
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", roleName ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", roleName ) );
            transaction.execute( String.format( "GRANT WRITE ON GRAPH * TO %s", roleName ) );
            transaction.commit();
        }

        // Then
        testSuccessfulRead( subject, 3 );
        testSuccessfulWrite( subject );

        // When
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "REVOKE WRITE ON GRAPH * FROM %s", roleName ) );
            transaction.commit();
        }

        // Then
        testSuccessfulRead( subject, 4 );
        testFailWrite( subject );
    }

    @Test
    void shouldAllowChangingBuiltinRoles()
    {
        for ( String role : Arrays.asList( PredefinedRoles.ADMIN, PredefinedRoles.ARCHITECT, PredefinedRoles.PUBLISHER, PredefinedRoles.EDITOR,
                PredefinedRoles.READER ) )
        {
            GraphDatabaseFacade systemGraph = neo.getSystemGraph();
            try ( Transaction tx = systemGraph.beginTx() )
            {
                tx.execute( String.format( "GRANT READ {foo} ON GRAPH * TO %s", role ) );
                tx.commit();
            }
            try ( Transaction tx = systemGraph.beginTx() )
            {
                tx.execute( String.format( "REVOKE READ {foo} ON GRAPH * FROM %s", role ) );
                tx.commit();
            }
        }
    }

    // Read properties test

    @Test
    void shouldOnlyShowWhitelistedProperties() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES Node TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertSuccess( subject, "MATCH (n:Node) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3 ) );
        assertSuccess( subject, "MATCH (n) return n.number", r -> assertKeyIs( r, "n.number", 0, 1, 2, 3, 4 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
    }

    @Test
    void shouldOnlyAllowReadOnWhitelistedProperties() throws Throwable
    {
        setupGraph();
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES A, B, C TO %s", role ) );
            transaction.commit();
        }

        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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

        String roRole = "readOnlyRole";
        createUserWithRole( "readOnlyUser", roRole );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", roRole ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES Node TO %s", roRole ) );
            transaction.commit();
        }

        String rfRole = "readFindRole";
        createUserWithRole( "readFindUser", rfRole );
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", rfRole ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", rfRole ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * Nodes Node TO %s", rfRole ) );
            transaction.commit();
        }

        EnterpriseLoginContext readOnly = neo.login( "readOnlyUser", PASSWORD );
        EnterpriseLoginContext findAndRead = neo.login( "readFindUser", PASSWORD );

        String query = "MATCH (n:A) WHERE n.number IS NOT NULL return n.number";
        String queryIndex = "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number";

        assertSuccess( adminSubject, query, r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( findAndRead, query, r -> assertKeyIs( r, "n.number", 3 ) );
        assertEmpty( readOnly, query );

        assertEmpty( adminSubject, "CREATE INDEX FOR (n:A) ON (n.number)" );
        assertEmpty( adminSubject, "CALL db.awaitIndexes" );

        assertSuccess( adminSubject, queryIndex, r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( findAndRead, queryIndex, r -> assertKeyIs( r, "n.number", 3 ) );
        assertEmpty( readOnly, queryIndex );
    }

    @Test
    void shouldOnlyShowWhitelistedPropertiesWithIndexSeek() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE INDEX FOR (n:A) ON (n.number)" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );
        assertEmpty( adminSubject, "CREATE (n:Node:A) SET n.number = 3" );
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES Node TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES Node TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES Node TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( subject, "MATCH (n:A) WHERE n.number IS NOT NULL return n.number", r -> assertKeyIs( r, "n.number", 3 ) );
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT (a.number) IS NOT NULL" );
        assertSuccess( adminSubject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number",
                r -> assertKeyIs( r, "n.number", 3, 4 ) );
        assertSuccess( subject, "MATCH (n:A) USING INDEX n:A(number) WHERE n.number IS NOT NULL return n.number", r -> assertKeyIs( r, "n.number", 3 ) );
    }

    @Test
    void shouldRespectExistsConstraintsWithoutReadPrivileges() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT (a.number) IS NOT NULL" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT WRITE ON GRAPH * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertFail( adminSubject, "CREATE (:A)", "with label `A` must have the property `number`" );
        assertFail( subject, "CREATE (:A)", "with label `A` must have the property `number`" );
        assertSuccess( adminSubject, "MATCH (a:A) RETURN count(a)", r -> assertKeyIs( r, "count(a)", 1 ));
    }

    @Test
    void shouldRespectUniqueConstraintsWithoutReadPrivileges() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE CONSTRAINT ON (a:A) ASSERT a.number IS UNIQUE" );
        assertEmpty( adminSubject, "CREATE (n:A) SET n.number = 4" );

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT WRITE ON GRAPH * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertFail( adminSubject, "CREATE (:A {number: 4})", "already exists with label `A` and property `number` = 4" );
        assertFail( subject, "CREATE (:A {number: 4})", "already exists with label `A` and property `number` = 4" );
        assertSuccess( adminSubject, "MATCH (a:A) RETURN count(a)", r -> assertKeyIs( r, "count(a)", 1 ));
    }

    @Test
    void shouldOnlyShowPropertiesOnWhitelistedNodeLabelsForPattern() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (n:A {number: 0}) MERGE (n)-[:R]->(:B {number: 1}) MERGE (n)-[:R]->(:C {number: 2})" );
        assertEmpty( adminSubject, "CREATE (n:A {number: 3})" );

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES A, B TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES A, B TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES A, B, C TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT WRITE ON GRAPH * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertEmpty( adminSubject, "CREATE (:A {foo: 1})" );

        String query = "CREATE (:A {foo: $param}) WITH 1 as bar MATCH (a:A) RETURN a.foo";
        assertSuccess( adminSubject, DEFAULT_DATABASE_NAME, query, Collections.singletonMap("param", 2L), r -> assertKeyIs( r, "a.foo", 1, 2 ) );
        assertSuccess( subject, DEFAULT_DATABASE_NAME, query, Collections.singletonMap("param", 3L), r -> assertKeyIs( r, "a.foo", null, null, 3 ) );
    }

    @Test
    void shouldReadBackIndexedNodeCreatedInSameTransaction() throws Throwable
    {
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT WRITE ON GRAPH * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        setupGraph();
        assertEmpty( adminSubject, "CREATE (:A {foo: 1})" );

        assertEmpty( adminSubject, "CREATE INDEX FOR (n:A) ON (n.foo)" );
        assertEmpty( adminSubject, "CALL db.awaitIndexes" );

        String query = "CREATE (:A {foo: $param}) WITH 1 as bar MATCH (a:A) USING INDEX a:A(foo) WHERE EXISTS(a.foo) RETURN a.foo";
        assertSuccess( adminSubject, DEFAULT_DATABASE_NAME, query,Collections.singletonMap("param", 2L), r -> assertKeyIs( r, "a.foo", 1, 2 ) );
        assertSuccess( subject, DEFAULT_DATABASE_NAME, query, Collections.singletonMap("param", 3L), r -> assertKeyIs( r, "a.foo", 3 ) );
    }

    @Test
    void shouldOnlyShowWhitelistedPropertiesForSegment() throws Throwable
    {
        String role1 = "role1";
        createUserWithRole( "user1", role1 );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role1 ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * NODES A TO %s", role1 ) );
            transaction.execute( String.format( "GRANT READ {foo} ON GRAPH * NODES A TO %s", role1 ) );
            transaction.execute( String.format( "GRANT READ {bar} ON GRAPH * NODES B TO %s", role1 ) );
            transaction.commit();
        }
        EnterpriseLoginContext findSome = neo.login( "user1", PASSWORD );

        String role2 = "role2";
        createUserWithRole( "user2", role2 );
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role2 ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role2 ) );
            transaction.execute( String.format( "GRANT READ {foo} ON GRAPH * NODES A TO %s", role2 ) );
            transaction.execute( String.format( "GRANT READ {bar} ON GRAPH * NODES B TO %s", role2 ) );
            transaction.commit();
        }
        EnterpriseLoginContext findAll = neo.login( "user2", PASSWORD );

        assertEmpty( adminSubject, "MATCH (n) DETACH DELETE n" );
        assertEmpty( adminSubject, "CREATE (:A {foo: 1, bar: 2}), (:B {foo: 3, bar: 4}), (:A:B {foo: 5, bar: 6}), ({foo: 7, bar: 8})" );

        assertSuccess( adminSubject, "MATCH (n) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, 3, 5, 7 ) );
        assertSuccess( findAll, "MATCH (n) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, null, 5, null ) );
        assertSuccess( findSome, "MATCH (n) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, 5 ) );

        assertSuccess( adminSubject, "MATCH (n:A) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 1, 5 ) );
        assertSuccess( findAll, "MATCH (n:A) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 5 ) );
        assertSuccess( findSome, "MATCH (n:A) WHERE exists(n.foo) AND exists(n.bar) RETURN n.foo", r -> assertKeyIs( r, "n.foo", 5 ) );

        assertEmpty( adminSubject, "CREATE INDEX FOR (n:A) ON (n.foo, n.bar)" );
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
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * NODES A TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * NODES A TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 5 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 2 ) );
        assertSuccess( adminSubject, "MATCH (n:A) return count(n)", r -> assertKeyIs( r, "count(n)", 2 ) );
        assertSuccess( subject, "MATCH (n:A) return count(n)", r -> assertKeyIs( r, "count(n)", 2 ) );
        assertSuccess( adminSubject, "MATCH (n:Node) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
        assertSuccess( subject, "MATCH (n:Node) return count(n)", r -> assertKeyIs( r, "count(n)", 1 ) );
        assertSuccess( adminSubject, "MATCH (n:NonExistent) return count(n)", r -> assertKeyIs( r, "count(n)", 0 ) );
        assertSuccess( subject, "MATCH (n:NonExistent) return count(n)", r -> assertKeyIs( r, "count(n)", 0 ) );
    }

    @Test
    void shouldNotTraverseExplicitLabelsWithoutPermission() throws Throwable
    {
        setupGraph();
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * NODES A, C, D TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT READ {*} ON GRAPH * NODES A, C, D TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertSuccess( adminSubject, "MATCH (a:A)--(n1:D)--(c:C) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( subject, "MATCH (a:A)--(n1:D)--(c:C) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 0 ) );
        assertSuccess( adminSubject, "MATCH (a:A)--(n1:D)--(c:C)--(b:B) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 1 ) );
        assertSuccess( subject, "MATCH (a:A)--(n1:D)--(c:C)--(b:B) RETURN count(*)", r -> assertKeyIs( r, "count(*)", 0 ) );
    }

    @Test
    void shouldOnlyFindWhitelistedNodeLabelsForVarLengthPattern() throws Throwable
    {
        setupGraph();
        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * NODES A, B, C TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * NODES A, B, C TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

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

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * NODES A TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        assertSuccess( adminSubject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
        assertSuccess( subject, "MATCH (n) return count(n)", r -> assertKeyIs( r, "count(n)", 4 ) );
    }

    @Test
    void shouldReadPropertiesOnRelationship() throws Throwable
    {
        assertEmpty( adminSubject, "CREATE (:A)-[:REL {foo: 1}]->(:B)" );

        String role = "custom";
        createUserWithRole( "Alice", role );
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT ACCESS ON DATABASE * TO %s", role ) );
            transaction.execute( String.format( "GRANT TRAVERSE ON GRAPH * TO %s", role ) );
            transaction.commit();
        }
        EnterpriseLoginContext subject = neo.login( "Alice", PASSWORD );

        String query = "MATCH (:A)-[r]->(:B) RETURN r.foo";

        assertSuccess( adminSubject, query, r -> assertKeyIs( r, "r.foo", 1 ) );
        assertSuccess( subject, query, r -> {
            List<Map<String,Object>> rows = r.stream().collect( Collectors.toList() );
            assertThat( rows.size(), equalTo( 1 ) );
            assertNull( rows.get( 0 ).get( "r.foo" ) );
        } );

        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "GRANT READ {foo} ON GRAPH * RELATIONSHIPS REL TO %s", role ) );
            transaction.commit();
        }

        assertSuccess( subject, query, r -> assertKeyIs( r, "r.foo", 1 ) );
    }

    // Helpers

    private void setupGraph()
    {
        // (a:A)-[:R]->(:B)-[:R]->(:B)-[:R]->(:C)<-[:R]-(:D)<-[:R]-(a)
        assertEmpty( adminSubject,
                "CREATE (a:A {number: 0}) " +
                        "MERGE (a)-[:R]->(:B {number: 1})-[:R]->(b:B {number: 2})-[:R]-(c:C {number: 3}) " +
                        "MERGE (a)-[:R]->(:D {number: 4})-[:R]->(c)" );
    }

    private void createUserWithRole( String username, String role )
    {
        GraphDatabaseFacade systemGraph = neo.getSystemGraph();
        try ( Transaction transaction = systemGraph.beginTx() )
        {
            transaction.execute( String.format( "CREATE USER %s SET PASSWORD '%s' CHANGE NOT REQUIRED", username, PASSWORD ) );
            transaction.execute( String.format( "CREATE ROLE %s", role ) );
            transaction.execute( String.format( "GRANT ROLE %s TO %s", role, username ) );
            transaction.commit();
        }
    }
}
