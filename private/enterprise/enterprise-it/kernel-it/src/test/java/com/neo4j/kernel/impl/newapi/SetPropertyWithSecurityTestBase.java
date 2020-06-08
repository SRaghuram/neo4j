/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
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
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class SetPropertyWithSecurityTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    private int barLabelId;
    private int fooLabelId;
    private int rel1RelType;
    private int rel2RelType;
    private int prop1Type;
    private int prop2Type;
    private long fooNodeReference;
    private long barNodeReference;
    private long rel1Reference;
    private long rel2Reference;
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
            prop1Type = tokenWrite.propertyKeyGetOrCreateForName( "PROP1" );
            prop2Type = tokenWrite.propertyKeyGetOrCreateForName( "PROP2" );
            transaction.commit();
        }

        try ( KernelTransaction transaction = beginTransaction() )
        {
            Write write = transaction.dataWrite();
            fooNodeReference = write.nodeCreateWithLabels( new int[]{fooLabelId} );
            barNodeReference = write.nodeCreateWithLabels( new int[]{barLabelId} );
            write.nodeSetProperty( barNodeReference, prop2Type, Values.stringValue( "existing" ) );
            rel1Reference = write.relationshipCreate( fooNodeReference, rel1RelType, barNodeReference );
            rel2Reference = write.relationshipCreate( fooNodeReference, rel2RelType, barNodeReference );
            write.relationshipSetProperty( rel2Reference, prop2Type, Values.stringValue( "existing" ) );
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
            tx.execute( "GRANT SET PROPERTY { PROP1 } ON GRAPH * NODE foo TO testRole" );
            tx.execute( "GRANT SET PROPERTY { PROP1 } ON GRAPH * RELATIONSHIP REL1 TO testRole" );

            tx.execute( "CREATE USER godUser SET PASSWORD 'abc123' CHANGE NOT REQUIRED" );
            tx.execute( "CREATE ROLE godRole" );
            tx.execute( "GRANT ROLE godRole TO godUser" );
            tx.execute( "GRANT MATCH {*} ON GRAPH * ELEMENTS * TO godRole" );
            tx.execute( "GRANT SET PROPERTY {*} ON GRAPH * TO godRole" );

            tx.commit();
        }
        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    @Test
    void shouldSetPropertyFooNode() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeSetProperty( fooNodeReference, prop1Type, Values.stringValue( "badger" ) );
            tx.commit();
        }

        assertNodeHasProperty( fooNodeReference, prop1Type, "badger" );
    }

    @Test
    void shouldNotSetPropertyFooNode() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception = assertThrows( AuthorizationViolationException.class,
                                                                      () -> tx.dataWrite()
                                                                              .nodeSetProperty( barNodeReference, prop1Type, Values.stringValue( "badger" ) ) );
            assertThat( exception.getMessage() ).contains( "Set property for property 'PROP1' is not allowed for " );

            tx.commit();
        }
        assertNodeDoesNotHaveProperty( barNodeReference, prop1Type );
    }

    @Test
    void shouldUpdatePropertyFooNode() throws Exception
    {
        // WHEN
        LoginContext loginContext = getLoginContext( "godUser" );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeSetProperty( fooNodeReference, prop1Type, Values.stringValue( "badger" ) );
            tx.commit();
        }

        loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeSetProperty( fooNodeReference, prop1Type, Values.stringValue( "snake" ) );
            tx.commit();
        }

        assertNodeHasProperty( fooNodeReference, prop1Type, "snake" );
    }

    @Test
    void shouldNotUpdatePropertyForNode() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception = assertThrows( AuthorizationViolationException.class,
                                                                      () -> tx.dataWrite()
                                                                              .nodeSetProperty( barNodeReference, prop2Type, Values.stringValue( "snake" ) ) );
            assertThat( exception.getMessage() ).contains( "Set property for property 'PROP2' is not allowed for " );
            tx.commit();
        }

        assertNodeHasProperty( barNodeReference, prop2Type, "existing" );
    }

    @Test
    void shouldSetExistingNodePropertyWithoutGrantIfIdentical() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeSetProperty( barNodeReference, prop2Type, Values.stringValue( "existing" ) );
            tx.commit();
        }

        assertNodeHasProperty( barNodeReference, prop2Type, "existing" );
    }

    @Test
    void shouldDeleteNonExistingNodePropertyWithoutGrant() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeRemoveProperty( barNodeReference, prop1Type );
            tx.commit();
        }

        assertNodeDoesNotHaveProperty( barNodeReference, prop1Type );
    }

    @Test
    void shouldDeletePropertyForNode() throws Exception
    {
        LoginContext loginContext = getLoginContext( "godUser" );

        assertNodeHasProperty( barNodeReference, prop2Type, "existing" );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().nodeRemoveProperty( barNodeReference, prop2Type );
            tx.commit();
        }

        assertNodeDoesNotHaveProperty( barNodeReference, prop2Type );
    }

    @Test
    void shouldNotDeletePropertyForNode() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();

        assertNodeHasProperty( barNodeReference, prop2Type, "existing" );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception = assertThrows( AuthorizationViolationException.class,
                                                                      () -> tx.dataWrite().nodeRemoveProperty( barNodeReference, prop2Type ) );
            assertThat( exception.getMessage() ).contains( "Set property for property 'PROP2' is not allowed for " );
            tx.commit();
        }
        assertNodeHasProperty( barNodeReference, prop2Type, "existing" );
    }

    @Test
    void shouldSetPropertyForRelationship() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().relationshipSetProperty( rel1Reference, prop1Type, Values.stringValue( "badger" ) );
            tx.commit();
        }

        assertRelationshipHasProperty( rel1Reference, prop1Type, "badger" );
    }

    @Test
    void shouldSetNotPropertyForRelationship() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception = assertThrows( AuthorizationViolationException.class,
                                                                      () -> tx.dataWrite().relationshipSetProperty( rel1Reference, prop2Type,
                                                                                                                    Values.stringValue( "badger" ) ) );
            assertThat( exception.getMessage() ).contains( "Set property for property 'PROP2' is not allowed for " );
            tx.commit();
        }

        assertRelationshipDoesNotHaveProperty( rel1Reference, prop2Type );
    }

    @Test
    void shouldUpdateExistingPropertyForRelationshipWithoutGrantIfIdentical() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().relationshipSetProperty( rel2Reference, prop2Type, Values.stringValue( "existing" ) );
            tx.commit();
        }

        assertRelationshipHasProperty( rel2Reference, prop2Type, "existing" );
    }

    @Test
    void shouldUpdatePropertyForRelationship() throws Exception
    {
        // GIVEN
        LoginContext loginContext = getLoginContext( "godUser" );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().relationshipSetProperty( rel1Reference, prop1Type, Values.stringValue( "badger" ) );
            tx.commit();
        }

        // WHEN
        loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().relationshipSetProperty( rel1Reference, prop1Type, Values.stringValue( "snake" ) );
            tx.commit();
        }

        // THEN
        assertRelationshipHasProperty( rel1Reference, prop1Type, "snake" );
    }

    @Test
    void shouldNotUpdatePropertyForRelationship() throws Exception
    {
        // WHEN
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().relationshipSetProperty( rel1Reference, prop1Type, Values.stringValue( "snake" ) );
            tx.commit();
        }

        // THEN
        assertRelationshipHasProperty( rel1Reference, prop1Type, "snake" );
    }

    @Test
    void shouldDeleteNonExistingPropertyForRelationshipWithoutGrant() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception = assertThrows( AuthorizationViolationException.class,
                                                                      () -> tx.dataWrite().relationshipRemoveProperty( rel2Reference, prop2Type ) );
            assertThat( exception.getMessage() ).contains( "Set property for property 'PROP2' is not allowed for " );
            tx.commit();
        }

        assertRelationshipHasProperty( rel2Reference, prop2Type, "existing" );
    }

    @Test
    void shouldDeletePropertyForRelationship() throws Exception
    {
        LoginContext loginContext = getLoginContext( "godUser" );
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            tx.dataWrite().relationshipRemoveProperty( rel2Reference, prop2Type );
            tx.commit();
        }

        assertRelationshipDoesNotHaveProperty( rel2Reference, prop2Type );
    }

    @Test
    void shouldNotDeletePropertyForRelationship() throws Exception
    {
        LoginContext loginContext = getTestUserLoginContext();
        try ( KernelTransaction tx = beginTransaction( loginContext ) )
        {
            AuthorizationViolationException exception = assertThrows( AuthorizationViolationException.class,
                                                                      () -> tx.dataWrite().relationshipRemoveProperty( rel2Reference, prop2Type ) );
            assertThat( exception.getMessage() ).contains( "Set property for property 'PROP2' is not allowed for " );
            tx.commit();
        }

        assertRelationshipHasProperty( rel2Reference, prop2Type, "existing" );
    }

    private void assertNodeHasProperty( long nodeReference, int propType, String value ) throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL );
                  PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE  ) )
            {
                tx.dataRead().singleNode( nodeReference, nodeCursor );
                nodeCursor.next();
                nodeCursor.properties( propertyCursor );
                assertTrue( propertyCursor.seekProperty( propType ) );
                assertEquals( propertyCursor.propertyValue(), Values.stringValue( value ) );
            }
            tx.commit();
        }
    }

    private void assertNodeDoesNotHaveProperty( long nodeReference, int propType ) throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( NodeCursor nodeCursor = tx.cursors().allocateNodeCursor( PageCursorTracer.NULL );
                  PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE  ) )
            {
                tx.dataRead().singleNode( nodeReference, nodeCursor );
                nodeCursor.next();
                nodeCursor.properties( propertyCursor );
                assertFalse( propertyCursor.seekProperty( propType ) );
            }
            tx.commit();
        }
    }

    private void assertRelationshipHasProperty( long relationshipReference, int propType, String value ) throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( RelationshipScanCursor relationshipCursor = tx.cursors().allocateRelationshipScanCursor( PageCursorTracer.NULL );
                  PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE  ) )
            {
                tx.dataRead().singleRelationship( relationshipReference, relationshipCursor );
                relationshipCursor.next();
                relationshipCursor.properties( propertyCursor );
                assertTrue( propertyCursor.seekProperty( propType ) );
                assertEquals( propertyCursor.propertyValue(), Values.stringValue( value ) );
            }
            tx.commit();
        }
    }

    private void assertRelationshipDoesNotHaveProperty( long relationshipReference, int propType ) throws Exception
    {
        try ( KernelTransaction tx = beginTransaction() )
        {
            try ( RelationshipScanCursor relationshipCursor = tx.cursors().allocateRelationshipScanCursor( PageCursorTracer.NULL );
                  PropertyCursor propertyCursor = tx.cursors().allocatePropertyCursor( PageCursorTracer.NULL, EmptyMemoryTracker.INSTANCE ) )
            {
                tx.dataRead().singleRelationship( relationshipReference, relationshipCursor );
                relationshipCursor.next();
                relationshipCursor.properties( propertyCursor );
                assertFalse( propertyCursor.seekProperty( propType ) );
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
