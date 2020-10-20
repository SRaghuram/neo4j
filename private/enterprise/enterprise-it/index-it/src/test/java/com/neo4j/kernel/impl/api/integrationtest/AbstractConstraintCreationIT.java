/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.EquivalentSchemaRuleAlreadyExistsException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
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
    DESCRIPTOR schema;

    abstract int initializeLabelOrRelType( TokenWrite tokenWrite, String name ) throws KernelException;

    abstract Constraint createConstraint( SchemaWrite writeOps, DESCRIPTOR descriptor ) throws Exception;

    abstract void createConstraintInRunningTx( SchemaHelper helper, GraphDatabaseService db, org.neo4j.graphdb.Transaction tx, String type, String property );

    abstract Constraint newConstraintObject( DESCRIPTOR descriptor );

    abstract void dropConstraint( SchemaWrite writeOps, Constraint constraint ) throws Exception;

    abstract void createOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx );

    abstract void removeOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx );

    abstract DESCRIPTOR makeDescriptor( int typeId, int propertyKeyId );

    @BeforeEach
    void createKeys() throws Exception
    {
        TokenWrite tokenWrite = tokenWriteInNewTransaction();
        this.typeId = initializeLabelOrRelType( tokenWrite, KEY );
        this.propertyKeyId = tokenWrite.propertyKeyGetOrCreateForName( PROP );
        this.schema = makeDescriptor( typeId, propertyKeyId );
        commit();
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( Path databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }

    @Test
    void shouldBeAbleToStoreAndRetrieveConstraint() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( transaction.schemaWrite(), schema );

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
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );

        // when
        ConstraintDescriptor constraint = createConstraint( transaction.schemaWrite(), schema );

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

        createConstraint( schemaWriteOperations, schema );

        // when
        rollback();

       KernelTransaction transaction = newTransaction();

        // then
        Iterator<?> constraints = transaction.schemaRead().constraintsGetAll();
        assertFalse( constraints.hasNext(), "should not have any constraints" );
        commit();
    }

    @Test
    void shouldNotStoreConstraintThatIsRemovedInTheSameTransaction() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AUTH_DISABLED );

        Constraint constraint = createConstraint( transaction.schemaWrite(), schema );

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
            constraint = createConstraint( statement, schema );
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
            KernelTransaction transaction = newTransaction();

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
            createConstraint( statement, schema );
            commit();
        }

        SchemaWrite statement = schemaWriteInNewTransaction();
        assertThrows( EquivalentSchemaRuleAlreadyExistsException.class, () -> createConstraint( statement, schema ) );
        commit();
    }

    @Test
    void shouldNotRemoveIndexFreeConstraintThatGetsReAdded() throws Exception
    {
        // given
        Constraint constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, schema );
            if ( constraint.isIndexBackedConstraint() )
            {
                // Ignore index-backed constraints in this test.
                return;
            }
            commit();
        }
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // Make sure all schema changes are stable, to avoid any synchronous schema state invalidation
            tx.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp(); // +1
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            constraint = createConstraint( statement, schema ); // clear
            commit(); // clear
        }
        {
           KernelTransaction transaction = newTransaction();

            // then
            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetAll();
            assertEquals( singletonList( constraint ), asCollection( constraints ) );
            schemaState.assertNotCleared( transaction );
            commit();
        }
    }

    @Test
    void shouldRemoveIndexBackedConstraintThatGetsReAdded() throws Exception
    {
        // given
        Constraint constraint;
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            constraint = createConstraint( statement, schema );
            if ( !constraint.isIndexBackedConstraint() )
            {
                // Ignore non-index-backed constraints in this test.
                return;
            }
            commit();
        }
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            // Make sure all schema changes are stable, to avoid any synchronous schema state invalidation
            tx.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
        }
        SchemaStateCheck schemaState = new SchemaStateCheck().setUp(); // +1
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            // when
            dropConstraint( statement, constraint );
            constraint = createConstraint( statement, schema ); // clear
            commit(); // clear
        }
        {
           KernelTransaction transaction = newTransaction();

            // then
            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetAll();
            assertEquals( singletonList( constraint ), asCollection( constraints ) );
            schemaState.assertCleared( transaction );
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
        createConstraint( statement, schema );
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
            constraint = createConstraint( statement, schema );
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
        Constraint constraint = newConstraintObject( schema );

        // when
        {
            SchemaWrite statement = schemaWriteInNewTransaction();

            var e = assertThrows( DropConstraintFailureException.class, () -> dropConstraint( statement, constraint ) );
            assertThat( e.getCause() ).isInstanceOf( NoSuchConstraintException.class );
            commit();
        }

        // then
        {
            KernelTransaction transaction = newTransaction();
            assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetAll() ) );
            commit();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldNotLeaveAnyStateBehindAfterFailingToCreateConstraint( SchemaHelper helper )
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( tx );
            tx.commit();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            var e = assertThrows( Exception.class, () ->
            {
                createConstraintInRunningTx( helper, db, tx, KEY, PROP );
                tx.commit();
            } );
            assertThat( e.getMessage() ).startsWith( "Unable to create Constraint" );
            assertThat( e ).isInstanceOfAny( ConstraintViolationException.class, QueryExecutionException.class );
        }

        // then
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            assertEquals( Collections.<ConstraintDefinition>emptyList(), asList( tx.schema().getConstraints() ) );
            assertEquals( Collections.<IndexDefinition, Schema.IndexState>emptyMap(), indexesWithState( tx.schema() ) );
            tx.commit();
        }
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void shouldBeAbleToResolveConflictsAndRecreateConstraintAfterFailingToCreateItDueToConflict( SchemaHelper helper )
            throws Exception
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            createOffendingDataInRunningTx( tx );
            tx.commit();
        }

        // when
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            var e = assertThrows( Exception.class, () ->
            {
                createConstraintInRunningTx( helper, db, tx, KEY, PROP );
                tx.commit();
            } );
            assertThat( e.getMessage() ).startsWith( "Unable to create Constraint" );
            assertThat( e ).isInstanceOfAny( ConstraintViolationException.class, QueryExecutionException.class );
        }

        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            removeOffendingDataInRunningTx( tx );
            tx.commit();
        }

        // then - this should not fail
        SchemaWrite statement = schemaWriteInNewTransaction();
        createConstraint( statement, schema );
        commit();
    }

    @ParameterizedTest
    @EnumSource( SchemaHelper.class )
    void changedConstraintsShouldResultInTransientFailure( SchemaHelper helper )
    {
        // Given
        Runnable constraintCreation = () ->
        {
            try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
            {
                createConstraintInRunningTx( helper, db, tx, KEY, PROP );
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
                    tx.createNode();
                    tx.commit();
                }
            } );

            // Then
            assertThat( e.getCause() ).isInstanceOf( TransactionFailureException.class );
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

        public SchemaStateCheck setUp()
        {
            KernelTransaction transaction = newTransaction();
            checkState( transaction );
            commit();
            return this;
        }

        void assertCleared( KernelTransaction transaction )
        {
            int count = invocationCount;
            checkState( transaction );
            assertEquals( count + 1, invocationCount, "schema state should have been cleared." );
        }

        void assertNotCleared( KernelTransaction transaction )
        {
            int count = invocationCount;
            checkState( transaction );
            assertEquals( count, invocationCount, "schema state should not have been cleared." );
        }

        private void checkState( KernelTransaction transaction )
        {
            assertEquals( Integer.valueOf( 7 ), transaction.schemaRead().schemaStateGetOrCreate( "7", this ) );
        }
    }
}
