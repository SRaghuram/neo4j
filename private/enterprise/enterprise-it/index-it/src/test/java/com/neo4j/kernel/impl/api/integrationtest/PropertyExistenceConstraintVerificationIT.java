/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;
import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.lang.reflect.Executable;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.ConstraintViolationTransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

@EnterpriseDbmsExtension
@ActorsExtension
abstract class PropertyExistenceConstraintVerificationIT
{
    private static final String KEY = "Foo";
    private static final String PROPERTY = "bar";

    @Inject
    public GraphDatabaseService db;
    @Inject
    public Actor thread;

    abstract void createConstraint( SchemaHelper helper, GraphDatabaseService db, Transaction tx, String key, String property );

    abstract Executable constraintCreationMethod() throws Exception;

    abstract void createOffender( Transaction tx, String key );

    abstract Executable offenderCreationMethod() throws Exception;

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldFailToCreateConstraintIfSomeNodeLacksTheMandatoryProperty( SchemaHelper helper )
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            createOffender( tx, KEY );
            tx.commit();
        }

        // when
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                createConstraint( helper, db, tx, KEY, PROPERTY );
                tx.commit();
            }
            fail( "expected exception" );
        }
        // then
        catch ( QueryExecutionException | ConstraintViolationException e )
        {
            assertThat( e.getMessage() ).startsWith( "Unable to create Constraint( type=" );
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldFailToCreateConstraintIfConcurrentlyCreatedEntityLacksTheMandatoryProperty( SchemaHelper helper ) throws Exception
    {
        // when
        try
        {
            Future<Void> nodeCreation;
            try ( Transaction tx = db.beginTx() )
            {
                createConstraint( helper, db, tx, KEY, PROPERTY );

                nodeCreation = thread.submit( createOffender() );
                thread.untilWaitingIn( offenderCreationMethod() );
                tx.commit();
            }
            nodeCreation.get();
            fail( "expected exception" );
        }
        // then, we either fail to create the constraint,
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getCause() ).isInstanceOf( CreateConstraintFailureException.class );
        }
        // or we fail to create the offending node
        catch ( ExecutionException e )
        {
            assertThat( e.getCause() ).isInstanceOf( ConstraintViolationException.class );
            assertThat( e.getCause().getCause() ).isInstanceOf( ConstraintViolationTransactionFailureException.class );
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldFailToCreateConstraintIfConcurrentlyCommittedEntityLacksTheMandatoryProperty( SchemaHelper helper ) throws Exception
    {
        // when
        try
        {
            Future<Void> constraintCreation;
            try ( Transaction tx = db.beginTx() )
            {
                createOffender( tx, KEY );

                constraintCreation = thread.submit( createConstraint( helper ) );
                thread.untilWaitingIn( constraintCreationMethod() );

                tx.commit();
            }
            constraintCreation.get();
            fail( "expected exception" );
        }
        // then, we either fail to create the constraint,
        catch ( ExecutionException e )
        {
            assertThat( e.getCause() ).isInstanceOfAny( QueryExecutionException.class, ConstraintViolationException.class );
            assertThat( e.getCause().getMessage() ).startsWith( "Unable to create Constraint( type=" );
        }
        // or we fail to create the offending node
        catch ( ConstraintViolationException e )
        {
            assertThat( e.getCause() ).isInstanceOf( ConstraintViolationTransactionFailureException.class );
        }
    }

    private Callable<Void> createOffender()
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                createOffender( tx, KEY );
                tx.commit();
            }
            return null;
        };
    }

    private Callable<Void> createConstraint( SchemaHelper helper )
    {
        return () ->
        {
            try ( Transaction tx = db.beginTx() )
            {
                createConstraint( helper, db, tx, KEY, PROPERTY );
                tx.commit();
            }
            return null;
        };
    }
}
