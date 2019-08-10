/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.schema.AlreadyConstrainedException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

abstract class AbstractConstraintCreationIT<Constraint extends ConstraintDescriptor, DESCRIPTOR extends SchemaDescriptor>
        extends KernelIntegrationTest
{
    static final String KEY = "Foo";
    static final String PROP = "bar";

    int typeId;
    int propertyKeyId;
    DESCRIPTOR descriptor;

    abstract int initializeLabelOrRelType( TokenWrite tokenWrite, String name )
            throws KernelException;

    abstract Constraint createConstraint( SchemaWrite writeOps, DESCRIPTOR descriptor ) throws Exception;

    abstract void createConstraintInRunningTx( GraphDatabaseService db, String type, String property );

    abstract Constraint newConstraintObject( DESCRIPTOR descriptor );

    abstract void dropConstraint( SchemaWrite writeOps, Constraint constraint ) throws Exception;

    abstract void createOffendingDataInRunningTx( GraphDatabaseService db );

    abstract void removeOffendingDataInRunningTx( GraphDatabaseService db );

    abstract DESCRIPTOR makeDescriptor( int typeId, int propertyKeyId );

    @BeforeEach
    void createKeys() throws Exception
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        this.typeId = initializeLabelOrRelType( tokenWrite, KEY );
        this.propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( PROP );
        this.descriptor = makeDescriptor( typeId, propertyKeyId );
        commit();
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( File databaseRootDir )
    {
        return new TestCommercialDatabaseManagementServiceBuilder( databaseRootDir );
    }

    @Test
    void shouldBeAbleToStoreAndRetrieveConstraint() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( transaction.schemaWrite(), descriptor );

        // then
        assertEquals( constraint, single( transaction.schemaRead().constraintsGetAll() ) );

        // given
        commit();
        transaction = newTransaction();

        // when
        Iterator<?> constraints = transaction.schemaRead().constraintsGetAll();

        // then
        assertEquals( constraint, single( constraints ) );
        commit();
    }

    @Test
    void shouldBeAbleToStoreAndRetrieveConstraintAfterRestart() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( transaction.schemaWrite(), descriptor );

        // then
        assertEquals( constraint, single( transaction.schemaRead().constraintsGetAll() ) );

        // given
        commit();
        restartDb();
        transaction = newTransaction();

        // when
        Iterator<?> constraints = transaction.schemaRead().constraintsGetAll();

        // then
        assertEquals( constraint, single( constraints ) );

        commit();
    }

    @Test
    void shouldNotPersistConstraintCreatedInAbortedTransaction() throws Exception
    {
        // given
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();

        createConstraint( schemaWriteOperations, descriptor );

        // when
        rollback();

       Transaction transaction = newTransaction();

        // then
        Iterator<?> constraints = transaction.schemaRead().constraintsGetAll();
        assertFalse( constraints.hasNext(), "should not have any constraints" );
        commit();
    }

    @Test
    void shouldNotStoreConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AUTH_DISABLED );

        Constraint constraint = createConstraint( transaction.schemaWrite(), descriptor );

        // when
        dropConstraint( transaction.schemaWrite(), constraint );

        // then
        assertFalse( transaction.schemaRead().constraintsGetAll().hasNext(), "should not have any constraints" );

        // when
        commit();

        transaction = newTransaction();

        // then
        assertFalse( transaction.schemaRead().constraintsGetAll().hasNext(), "should not have any constraints" );
        commit();
    }

    @Test
    void shouldDropConstraint() throws Exception
    {
        // given
        Constraint constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();
        }

        // when
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            dropConstraint( statement, constraint );
            commit();
        }

        // then
        {
            Transaction transaction = newTransaction();

            // then
            assertFalse( transaction.schemaRead().constraintsGetAll().hasNext(), "should not have any constraints" );
            commit();
        }
    }

    @Test
    void shouldNotCreateConstraintThatAlreadyExists() throws Exception
    {
        // given
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            createConstraint( statement, descriptor );
            commit();
        }

        SchemaWrite statement = schemaWriteInNewTransaction();
        assertThrows( AlreadyConstrainedException.class, () -> createConstraint( statement, descriptor ) );
        commit();
    }

    @Test
    void shouldNotRemoveConstraintThatGetsReAdded() throws Exception
    {
        // given
        Constraint constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();
        }
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // Make sure all schema changes are stable, to avoid any synchronous schema state invalidation
            db.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            createConstraint( statement, descriptor );
            commit();
        }
        {
           Transaction transaction = newTransaction();

            // then
            assertEquals( singletonList( constraint ), asCollection( transaction.schemaRead().constraintsGetAll() ) );
            schemaState.assertNotCleared( transaction );
            commit();
        }
    }

    @Test
    void shouldClearSchemaStateWhenConstraintIsCreated() throws Exception
    {
        // given
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp();

        SchemaWrite statement = schemaWriteInNewTransaction();

        // when
        createConstraint( statement, descriptor );
        commit();

        // then
        schemaState.assertCleared( newTransaction() );
        rollback();
    }

    @Test
    void shouldClearSchemaStateWhenConstraintIsDropped() throws Exception
    {
        // given
        Constraint constraint;
        SchemaStateCheck schemaState;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, descriptor );
            commit();

            schemaState = new SchemaStateCheck().setUp();
        }

        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            commit();
        }

        // then
        schemaState.assertCleared( newTransaction() );
        rollback();
    }

    @Test
    void shouldNotDropConstraintThatDoesNotExist() throws Exception
    {
        // given
        Constraint constraint = newConstraintObject( descriptor );

        // when
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            var e = assertThrows( DropConstraintFailureException.class, () -> dropConstraint( statement, constraint ) );
            assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );
            commit();
        }

        // then
        {
            Transaction transaction = newTransaction();
            assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetAll() ) );
            commit();
        }
    }

    @Test
    void shouldNotLeaveAnyStateBehindAfterFailingToCreateConstraint()
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( db );
            tx.commit();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            var e = assertThrows( QueryExecutionException.class, () ->
            {
                createConstraintInRunningTx( db, KEY, PROP );
                tx.commit();
            } );
            assertThat( e.getMessage(), startsWith( "Unable to create CONSTRAINT" ) );
        }

        // then
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            assertEquals( Collections.<ConstraintDefinition>emptyList(), asList( db.schema().getConstraints() ) );
            assertEquals( Collections.<IndexDefinition, Schema.IndexState>emptyMap(), indexesWithState( db.schema() ) );
            tx.commit();
        }
    }

    @Test
    void shouldBeAbleToResolveConflictsAndRecreateConstraintAfterFailingToCreateItDueToConflict()
            throws Exception
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( db );
            tx.commit();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            var e = assertThrows( QueryExecutionException.class, () ->
            {
                createConstraintInRunningTx( db, KEY, PROP );
                tx.commit();
            } );
            assertThat( e.getMessage(), startsWith( "Unable to create CONSTRAINT" ) );
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            removeOffendingDataInRunningTx( db );
            tx.commit();
        }

        // then - this should not fail
        SchemaWrite statement = schemaWriteInNewTransaction();
        createConstraint( statement, descriptor );
        commit();
    }

    @Test
    void changedConstraintsShouldResultInTransientFailure()
    {
        // Given
        Runnable constraintCreation = () ->
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                createConstraintInRunningTx( db, KEY, PROP );
                tx.commit();
            }
        };

        // When
        var executorService = Executors.newSingleThreadExecutor();
        try
        {
            var e = assertThrows( TransientTransactionFailureException.class, () ->
            {
                try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
                {
                    executorService.submit( constraintCreation ).get();
                    db.createNode();
                    tx.commit();
                }
            } );

            // Then
            assertThat( e.getCause(), instanceOf( TransactionFailureException.class ) );
            TransactionFailureException cause = (TransactionFailureException) e.getCause();
            assertEquals( Status.Transaction.ConstraintsChanged, cause.status() );
        }
        finally
        {
            executorService.shutdown();
        }
    }

    private static Map<IndexDefinition,Schema.IndexState> indexesWithState( Schema schema )
    {
        Map<IndexDefinition,Schema.IndexState> result = new HashMap<>();
        for ( IndexDefinition definition : schema.getIndexes() )
        {
            result.put( definition, schema.getIndexState( definition ) );
        }
        return result;
    }

    private class SchemaStateCheck implements Function<String,Integer>
    {
        int invocationCount;

        @Override
        public Integer apply( String s )
        {
            invocationCount++;
            return Integer.parseInt( s );
        }

        public SchemaStateCheck setUp() throws TransactionFailureException
        {
            Transaction transaction = newTransaction();
            checkState( transaction );
            commit();
            return this;
        }

        void assertCleared( Transaction transaction )
        {
            int count = invocationCount;
            checkState( transaction );
            assertEquals( count + 1, invocationCount, "schema state should have been cleared." );
        }

        void assertNotCleared( Transaction transaction )
        {
            int count = invocationCount;
            checkState( transaction );
            assertEquals( count, invocationCount, "schema state should not have been cleared." );
        }

        private void checkState( Transaction transaction )
        {
            assertEquals( Integer.valueOf( 7 ), transaction.schemaRead().schemaStateGetOrCreate( "7", this ) );
        }
    }
}
