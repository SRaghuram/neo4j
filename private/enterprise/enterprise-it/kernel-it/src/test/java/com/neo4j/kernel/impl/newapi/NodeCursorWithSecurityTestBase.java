/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.IntStream;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.RelationshipSelection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public abstract class NodeCursorWithSecurityTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static long foo;
    private static AuthManager authManager;
    private static long denseFoo;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // (foo :Foo), (bar :Bar), (baz: Baz), (denseFoo :Foo)
            // (foo)-[:B]->(bar)
            // (foo)<-[:A]-(bar)

            // (foo)-[:C]->(bar)
            // (foo)-[:A]->(baz)

            // (denseFoo)-[:A]->(:Bar) x100
            // (denseFoo)-[:C]->(bar)
            // (denseFoo)-[:A]->(baz)

            Node fooNode = tx.createNode( label( "Foo" ) );
            Node denseFooNode = tx.createNode( label( "Foo" ) );
            Node barNode = tx.createNode( label( "Bar" ) );
            Node bazNode = tx.createNode( label( "Baz" ) );

            // Traversable relationships
            IntStream.range( 0, 100 ).forEach( x -> denseFooNode.createRelationshipTo( tx.createNode( label( "Bar" ) ), RelationshipType.withName( "A" ) ) );
            fooNode.createRelationshipTo( barNode, RelationshipType.withName( "A" ) ).getId();
            fooNode.createRelationshipTo( barNode, RelationshipType.withName( "B" ) ).getId();
            barNode.createRelationshipTo( fooNode, RelationshipType.withName( "A" ) ).getId();

            // Not traversable relationship type
            denseFooNode.createRelationshipTo( barNode, RelationshipType.withName( "C" ) ).getId();
            fooNode.createRelationshipTo( barNode, RelationshipType.withName( "C" ) ).getId();

            // Not traversable end node
            denseFooNode.createRelationshipTo( bazNode, RelationshipType.withName( "A" ) ).getId();
            fooNode.createRelationshipTo( bazNode, RelationshipType.withName( "A" ) ).getId();

            foo = fooNode.getId();
            denseFoo = denseFooNode.getId();

            tx.createNode().getId();

            tx.commit();
        }
    }

    @Override
    public void createSystemGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.execute( "CREATE USER testUser SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE testRole" );
            tx.execute( "GRANT ROLE testRole TO testUser" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES Foo, Bar TO testRole" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS A, B TO testRole" );
            tx.execute( "DENY TRAVERSE ON GRAPH * NODES Baz TO testRole" );

            tx.execute( "CREATE USER userDeniedLabel SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE testRole2 AS COPY OF reader" );
            tx.execute( "GRANT ROLE testRole2 TO userDeniedLabel" );
            tx.execute( "DENY TRAVERSE ON GRAPH * NODES Baz TO testRole2" );

            tx.commit();
        }
        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    @Test
    void getDegree() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        try ( NodeCursor node = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( foo, node );
            node.next();
            int degree = node.degree( RelationshipSelection.selection( Direction.OUTGOING ) );

            // then
            assertThat( degree, equalTo( 2 ) );
        }
    }

    @Test
    void getDegreeDenseNode() throws Throwable
    {
        // given
        changeUser( getLoginContext() );

        try ( NodeCursor node = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( denseFoo, node );
            node.next();
            int degree = node.degree( RelationshipSelection.selection( Direction.OUTGOING ) );

            // then
            assertThat( degree, equalTo( 100 ) );
        }
    }

    @Test
    void getDegreeUserDenyTraverseOneLabel() throws Throwable
    {
        // given
        changeUser( getLoginContext( "userDeniedLabel" ) );

        try ( NodeCursor node = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( foo, node );
            node.next();
            int degree = node.degree( RelationshipSelection.selection( Direction.OUTGOING ) );

            // then
            assertThat( degree, equalTo( 3 ) );
        }
    }

    @Test
    void getDegreeDenseNodeUserDenyTraverseOneLabel() throws Throwable
    {
        // given
        changeUser( getLoginContext( "userDeniedLabel" ) );

        try ( NodeCursor node = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( denseFoo, node );
            node.next();
            int degree = node.degree( RelationshipSelection.selection( Direction.OUTGOING ) );

            // then
            assertThat( degree, equalTo( 101 ) );
        }
    }

    private LoginContext getLoginContext() throws InvalidAuthTokenException
    {
        return getLoginContext( "testUser" );
    }

    private LoginContext getLoginContext( String username ) throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", username, "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }
}
