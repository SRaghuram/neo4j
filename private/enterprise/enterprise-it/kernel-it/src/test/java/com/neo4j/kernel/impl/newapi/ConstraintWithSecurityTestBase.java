/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.newapi;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestBase;
import org.neo4j.kernel.impl.newapi.KernelAPIReadTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;

public abstract class ConstraintWithSecurityTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{

    private long bar;
    private long fooAbar, fooBbar;
    private int barLabel;
    private int aType, bType;
    private int prop1Key, prop2Key;
    private AuthManager authManager;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {

        try ( Transaction tx = graphDb.beginTx() )
        {
            // traversable
            Node fooNode = tx.createNode( label( "Foo" ) );
            // not traversable
            Node barNode = tx.createNode( label( "Bar" ) );
            barNode.setProperty( "prop2", 1 );
            Node bar2Node = tx.createNode( label( "Bar" ) );
            bar2Node.setProperty( "prop2", 1 );

            // not traversable because of type
            Relationship fooAbarRel = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "A" ) );
            // not traversable because of end node (type is traversable)
            Relationship fooBbarRel = fooNode.createRelationshipTo( barNode, RelationshipType.withName( "B" ) );

            fooAbar = fooAbarRel.getId();
            fooBbar = fooBbarRel.getId();

            bar = barNode.getId();

            tx.commit();
        }

        Kernel kernel = testSupport.kernelToTest();
        try ( KernelTransaction tx = kernel.beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            Token token = tx.token();
            barLabel = token.labelGetOrCreateForName( "Bar" );
            aType = token.relationshipTypeGetOrCreateForName( "A" );
            bType = token.relationshipTypeGetOrCreateForName( "B" );
            prop1Key = token.propertyKeyGetOrCreateForName( "prop1" );
            prop2Key = token.propertyKeyGetOrCreateForName( "prop2" );
            tx.commit();
        }
        catch ( KernelException e )
        {
            fail( e );
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
            tx.execute( "GRANT ACCESS ON DATABASE * TO testRole" );
            tx.execute( "GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO testRole" );
            tx.execute( "DENY TRAVERSE ON GRAPH * NODES Foo TO testRole" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * NODES Bar TO testRole" );
            tx.execute( "DENY TRAVERSE ON GRAPH * RELATIONSHIPS A TO testRole" );
            tx.execute( "GRANT TRAVERSE ON GRAPH * RELATIONSHIPS B TO testRole" );
            tx.execute( "DENY READ {prop1} ON GRAPH * NODES Bar TO testRole" );
            tx.execute( "DENY READ {prop1} ON GRAPH * RELATIONSHIPS A TO testRole" );

            tx.commit();
        }
        authManager = ((GraphDatabaseAPI) graphDb).getDependencyResolver().resolveDependency( AuthManager.class );
    }

    @BeforeEach
    void setUp() throws Exception
    {
        try ( KernelTransaction transaction = beginTransaction() )
        {
            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetAll();
            SchemaWrite schemaWrite = transaction.schemaWrite();
            while ( constraints.hasNext() )
            {
                schemaWrite.constraintDrop( constraints.next() );
            }
        }
    }

    @Test
    void shouldFailToCreateUniquePropertyConstraintWithNotTraversableNode() throws Throwable
    {
        changeUser( getLoginContext() );

        assertThatThrownBy(
                () -> tx.schemaWrite().uniquePropertyConstraintCreate( uniqueForSchema( forLabel( barLabel, prop2Key ) ) ) )
                .isInstanceOf( CreateConstraintFailureException.class ).hasMessageContaining( "Existing data does not satisfy Constraint", bar );
    }

    @Test
    void shouldFailToCreateNodeKeyConstraintWithNotTraversableNode() throws Throwable
    {
        changeUser( getLoginContext() );

        assertThatThrownBy(
                () -> tx.schemaWrite().nodeKeyConstraintCreate( uniqueForSchema( forLabel( barLabel, prop2Key ) ) ) )
                .isInstanceOf( CreateConstraintFailureException.class ).hasMessageContaining( "Existing data does not satisfy Constraint", bar );
    }

    @Test
    void shouldFailToCreateExistenceConstraintWithNotTraversableNode() throws Throwable
    {
        changeUser( getLoginContext() );

        assertThatThrownBy(
                () -> tx.schemaWrite().nodePropertyExistenceConstraintCreate( forLabel( barLabel, prop1Key ), "name" ) )
                .isInstanceOf( CreateConstraintFailureException.class ).hasMessageContaining( "Node(%d) does not satisfy Constraint", bar );
    }

    @Test
    void shouldFailToCreateExistenceConstraintWithNotTraversableRelationship() throws Throwable
    {
        changeUser( getLoginContext() );

        assertThatThrownBy(
                () -> tx.schemaWrite().relationshipPropertyExistenceConstraintCreate( forRelType( aType, prop1Key ), "name" ) )
                .isInstanceOf(
                        CreateConstraintFailureException.class ).hasMessageContaining( "Relationship(%d) does not satisfy Constraint", fooAbar );
    }

    @Test
    void shouldFailToCreateExistenceConstraintWithNotTraversableRelationshipBecauseOfEndNode() throws Throwable
    {
        changeUser( getLoginContext() );

        assertThatThrownBy(
                () -> tx.schemaWrite().relationshipPropertyExistenceConstraintCreate( forRelType( bType, prop1Key ), "name" ) )
                .isInstanceOf(
                        CreateConstraintFailureException.class ).hasMessageContaining( "Relationship(%d) does not satisfy Constraint", fooBbar );
    }

    protected LoginContext getLoginContext() throws InvalidAuthTokenException
    {
        return authManager.login( Map.of( "principal", "testUser", "credentials", "abc123".getBytes( StandardCharsets.UTF_8 ), "scheme", "basic" ) );
    }
}
