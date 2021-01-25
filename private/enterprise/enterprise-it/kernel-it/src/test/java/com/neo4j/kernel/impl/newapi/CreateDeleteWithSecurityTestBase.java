/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIWriteTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class CreateDeleteWithSecurityTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private int barLabelId;
    private int fooLabelId;
    private int rel1RelType;
    private int rel2RelType;
    private long nodeReference;
    private static AuthManager authManager;

    @BeforeEach
    void setUp() throws Exception
    {
        try ( KernelTransaction transaction = beginTransaction() )
        {
            TokenWrite tokenWrite = transaction.tokenWrite();
            fooLabelId = tokenWrite.labelGetOrCreateForName( "foo" );
            barLabelId = tokenWrite.labelGetOrCreateForName( "bar" );
            rel1RelType = tokenWrite.relationshipTypeGetOrCreateForName( "REL1" );
            rel2RelType = tokenWrite.relationshipTypeGetOrCreateForName( "REL2" );
            transaction.commit();
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
            tx.execute( "GRANT MATCH {*} ON GRAPH * ELEMENTS * TO testRole" );
            tx.execute( "GRANT CREATE ON GRAPH * NODE bar TO testRole" );
            tx.execute( "GRANT CREATE ON GRAPH * RELATIONSHIP REL1 TO testRole" );
            tx.execute( "GRANT DELETE ON GRAPH * NODE foo TO testRole" );
            tx.execute( "GRANT DELETE ON GRAPH * RELATIONSHIP REL1 TO testRole" );

            tx.execute( "CREATE USER godUser SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE godRole" );
            tx.execute( "GRANT ROLE godRole TO godUser" );
            tx.execute( "GRANT MATCH {*} ON GRAPH * ELEMENTS * TO godRole" );
            tx.execute( "GRANT CREATE ON GRAPH * TO godRole" );
            tx.execute( "GRANT DELETE ON GRAPH * TO godRole" );

            tx.commit();
        }
        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    @Test
    void shouldCreateEmptyNode() throws Exception
    {
        LoginContext loginContext = getLoginContext( "godUser" );
        successfullyCreateNode( tx -> tx.dataWrite().nodeCreate(), loginContext );
    }

    @Test
    void shouldCreateBarNode() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        successfullyCreateNode( tx -> tx.dataWrite().nodeCreateWithLabels( new int[]{barLabelId} ), loginContext );
    }

    @Test
    void shouldFailToCreateEmptyNode() throws Exception
    {
        failToCreateNode( tx -> tx.dataWrite().nodeCreate(), "Create node with labels '' is not allowed for " );
    }

    @Test
    void shouldFailToCreateFooNode() throws Exception
    {
        failToCreateNode( tx -> tx.dataWrite().nodeCreateWithLabels( new int[]{fooLabelId} ), "Create node with labels 'foo' is not allowed for " );
    }

    @Test
    void shouldFailToCreateFooBarNode() throws Exception
    {
        failToCreateNode( tx -> tx.dataWrite().nodeCreateWithLabels( new int[]{fooLabelId, barLabelId} ),
                          "Create node with labels 'foo,bar' is not allowed for " );
    }

    @Test
    void shouldCreateRelationship() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();

        // GIVEN
        long node1, node2;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node1 = tx.dataWrite().nodeCreate();
            node2 = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        // WHEN
        long relId;
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            relId = tx.dataWrite().relationshipCreate( node1, rel1RelType, node2 );
            tx.commit();
        }

        // THEN
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( RelationshipScanCursor relCursor = tx.cursors().allocateRelationshipScanCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleRelationship( relId, relCursor );
                assertTrue( relCursor.next() );
            }
            tx.commit();
        }
    }

    @Test
    void failToCreateRelationship() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();

        // GIVEN
        long node1, node2;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node1 = tx.dataWrite().nodeCreateWithLabels( new int[]{barLabelId} );
            node2 = tx.dataWrite().nodeCreateWithLabels( new int[]{barLabelId} );
            tx.commit();
        }

        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception =
                    assertThrows( AuthorizationViolationException.class, () -> tx.dataWrite().relationshipCreate( node1, rel2RelType, node2 ) );
            assertThat( exception.getMessage() ).contains( "Create relationship with type 'REL2' is not allowed for " );
            tx.commit();
        }
    }

    @Test
    void shouldDeleteEmptyNode() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getLoginContext( "godUser" );
        long node;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node = tx.dataWrite().nodeCreate();
            tx.commit();
        }

        successfullyDeleteNode( tx -> tx.dataWrite().nodeDelete( node ), loginContext );
    }

    @Test
    void shouldDeleteFooNode() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getTestUserLoginContext();
        long node;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node = tx.dataWrite().nodeCreateWithLabels( new int[]{fooLabelId} );
            tx.commit();
        }

        successfullyDeleteNode( tx -> tx.dataWrite().nodeDelete( node ), loginContext );
    }

    @Test
    void failToDeleteBarNode() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getTestUserLoginContext();
        long node;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node = tx.dataWrite().nodeCreateWithLabels( new int[]{barLabelId} );
            tx.commit();
        }

        // WHEN
        failToDeleteNode( tx -> tx.dataWrite().nodeDelete( node ), loginContext, "Delete node with labels 'bar' is not allowed for " );
    }

    @Test
    void failToDeleteFooBarNode() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getTestUserLoginContext();
        long node;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node = tx.dataWrite().nodeCreateWithLabels( new int[]{fooLabelId, barLabelId} );
            tx.commit();
        }

        // WHEN
        failToDeleteNode( tx -> tx.dataWrite().nodeDelete( node ), loginContext, "Delete node with labels 'foo,bar' is not allowed for " );
    }

    @Test
    void successfullyDeleteRelationship() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getTestUserLoginContext();
        long relId;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            long node1 = tx.dataWrite().nodeCreate();
            long node2 = tx.dataWrite().nodeCreate();
            relId = tx.dataWrite().relationshipCreate( node1, rel1RelType, node2 );
            tx.commit();
        }

        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().relationshipDelete( relId );
            tx.commit();
        }

        // THEN
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( RelationshipScanCursor relCursor = tx.cursors().allocateRelationshipScanCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleRelationship( relId, relCursor );
                assertFalse( relCursor.next() );
            }
            tx.commit();
        }
    }

    @Test
    void failToDeleteRelationship() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getTestUserLoginContext();
        long relId;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            long node1 = tx.dataWrite().nodeCreate();
            long node2 = tx.dataWrite().nodeCreate();
            relId = tx.dataWrite().relationshipCreate( node1, rel2RelType, node2 );
            tx.commit();
        }

        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception =
                    assertThrows( AuthorizationViolationException.class, () -> tx.dataWrite().relationshipDelete( relId ) );
            assertThat( exception.getMessage() ).contains( "Delete relationship with type 'REL2' is not allowed for " );
            tx.commit();
        }

        // THEN
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( RelationshipScanCursor relCursor = tx.cursors().allocateRelationshipScanCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleRelationship( relId, relCursor );
                assertTrue( relCursor.next() );
            }
            tx.commit();
        }
    }

    @Test
    void successfullyDetachDeleteNode() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getTestUserLoginContext();
        long node1;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node1 = tx.dataWrite().nodeCreateWithLabels( new int[]{fooLabelId} );
            long node2 = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate( node1, rel1RelType, node2 );
            tx.commit();
        }

        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeDetachDelete( node1 );
            tx.commit();
        }

        // THEN
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleNode( node1, nodeCursor );
                assertFalse( nodeCursor.next() );
            }
            tx.commit();
        }
    }

    @Test
    void failToDetachDeleteNodeWithoutDeleteRelationshipPermission() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getTestUserLoginContext();
        long node1;
        try ( KernelTransaction tx = beginTransaction( LoginContext.AUTH_DISABLED ) )
        {
            node1 = tx.dataWrite().nodeCreateWithLabels( new int[]{fooLabelId} );
            long node2 = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate( node1, rel2RelType, node2 );
            tx.commit();
        }

        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception =
                    assertThrows( AuthorizationViolationException.class, () -> tx.dataWrite().nodeDetachDelete( node1 ) );
            assertThat( exception.getMessage() ).contains( "Delete relationship with type 'REL2' is not allowed" );
            tx.commit();
        }

        // THEN
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleNode( node1, nodeCursor );
                assertTrue( nodeCursor.next() );
            }
            tx.commit();
        }
    }

    private void failToCreateNode( ThrowingConsumer<KernelTransaction,Exception> command, String errormsg ) throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();

        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception =
                    assertThrows( AuthorizationViolationException.class, () -> command.accept( tx ) );
            assertThat( exception.getMessage() ).contains( errormsg );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().allNodesScan( nodeCursor );
                assertFalse( nodeCursor.next() );
            }
            tx.commit();
        }
    }

    private void successfullyCreateNode( ThrowingFunction<KernelTransaction,Long,Exception> command, LoginContext loginContext ) throws Exception
    {
        // WHEN
        long node;
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            node = command.apply( tx );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleNode( node, nodeCursor );
                assertTrue( nodeCursor.next() );
            }
            tx.commit();
        }
    }

    private void successfullyDeleteNode( ThrowingConsumer<KernelTransaction,Exception> command, LoginContext loginContext ) throws Exception
    {
        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            command.accept( tx );
            tx.commit();
        }

        // THEN
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().allNodesScan( nodeCursor );
                assertFalse( nodeCursor.next() );
            }
            tx.commit();
        }
    }

    private void failToDeleteNode( ThrowingConsumer<KernelTransaction,Exception> command, LoginContext loginContext, String errormsg ) throws Exception
    {
        // WHEN
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception =
                    assertThrows( AuthorizationViolationException.class, () -> command.accept( tx ) );
            assertThat( exception.getMessage() ).contains( errormsg );
            tx.commit();
        }

        // THEN
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().allNodesScan( nodeCursor );
                assertTrue( nodeCursor.next() );
            }
            tx.commit();
        }
    }

    private LoginContext getLoginContext( String username ) throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", username, "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }

    private LoginContext getTestUserLoginContext() throws InvalidAuthTokenException
    {
        return getLoginContext( "testUser" );
    }
}
