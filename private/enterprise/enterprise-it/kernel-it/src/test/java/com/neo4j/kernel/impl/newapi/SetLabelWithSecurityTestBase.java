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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
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

public abstract class SetLabelWithSecurityTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private int labelId;
    private int fooLabelId;
    private long nodeReference;
    private static AuthManager authManager;

    @BeforeEach
    void setUp() throws Exception
    {
        try ( KernelTransaction transaction = beginTransaction() )
        {
            TokenWrite tokenWrite = transaction.tokenWrite();
            labelId = tokenWrite.labelGetOrCreateForName( "label" );
            fooLabelId = tokenWrite.labelGetOrCreateForName( "foo" );
            transaction.commit();
        }

        try ( KernelTransaction transaction = beginTransaction() )
        {
            Write write = transaction.dataWrite();
            nodeReference = write.nodeCreate();
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
            tx.execute( "GRANT SET LABEL label ON GRAPH * TO testRole" );

            tx.commit();
        }
        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    @Test
    void shouldSetAllowedLabel() throws Exception
    {
        LoginContext loginContext = getLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeAddLabel( nodeReference, labelId );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleNode( nodeReference, nodeCursor );
                nodeCursor.next();
                assertTrue( nodeCursor.hasLabel( labelId ) );
            }
            tx.commit();
        }
    }

    @Test
    void shouldFailSetNotAllowedLabel() throws Exception
    {
        LoginContext loginContext = getLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception =
                    assertThrows( AuthorizationViolationException.class, () -> tx.dataWrite().nodeAddLabel( nodeReference, fooLabelId ) );
            assertThat( exception.getMessage() ).contains( "Set label for label 'foo' is not allowed for " );
            tx.commit();
        }

        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL ) )
            {
                tx.dataRead().singleNode( nodeReference, nodeCursor );
                nodeCursor.next();
                assertFalse( nodeCursor.hasLabel( fooLabelId ) );
            }
            tx.commit();
        }
    }

    private LoginContext getLoginContext() throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", "testUser", "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }
}
