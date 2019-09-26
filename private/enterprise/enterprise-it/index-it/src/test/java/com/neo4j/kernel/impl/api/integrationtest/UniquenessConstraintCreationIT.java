/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.api.integrationtest;

import com.neo4j.SchemaHelper;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.OptionalLong;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.stringContainsInOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.single;

class UniquenessConstraintCreationIT extends AbstractConstraintCreationIT<ConstraintDescriptor, LabelSchemaDescriptor>
{
    private static final String DUPLICATED_VALUE = "apa";
    private final AssertableLogProvider assertableLogProvider = new AssertableLogProvider();

    @Override
    protected TestDatabaseManagementServiceBuilder configure( TestDatabaseManagementServiceBuilder factory )
    {
        factory.setUserLogProvider( assertableLogProvider );
        factory.setInternalLogProvider( assertableLogProvider );
        return super.configure( factory );
    }

    @Override
    int initializeLabelOrRelType( TokenWrite tokenWrite, String name ) throws KernelException
    {
        return tokenWrite.labelGetOrCreateForName( KEY );
    }

    @Override
    ConstraintDescriptor createConstraint( SchemaWrite writeOps, LabelSchemaDescriptor descriptor )
            throws Exception
    {
        return writeOps.uniquePropertyConstraintCreate( descriptor, null );
    }

    @Override
    void createConstraintInRunningTx( SchemaHelper helper, GraphDatabaseService db, org.neo4j.graphdb.Transaction tx, String type, String property )
    {
        helper.createUniquenessConstraint( db, tx, type, property );
    }

    @Override
    UniquenessConstraintDescriptor newConstraintObject( LabelSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.uniqueForSchema( descriptor ).withName( "constraint" );
    }

    @Override
    void dropConstraint( SchemaWrite writeOps, ConstraintDescriptor constraint ) throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx )
    {
        tx.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
        tx.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
    }

    @Override
    void removeOffendingDataInRunningTx( org.neo4j.graphdb.Transaction tx )
    {
        try ( ResourceIterator<Node> nodes = tx.findNodes( label( KEY ), PROP, DUPLICATED_VALUE ) )
        {
            while ( nodes.hasNext() )
            {
                nodes.next().delete();
            }
        }
    }

    @Override
    LabelSchemaDescriptor makeDescriptor( int typeId, int propertyKeyId )
    {
        return SchemaDescriptor.forLabel( typeId, propertyKeyId );
    }

    @Test
    void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        // name is not unique for Foo in the existing data

        int foo = transaction.tokenWrite().labelGetOrCreateForName( "Foo" );
        int name = transaction.tokenWrite().propertyKeyGetOrCreateForName( "name" );

        long node1 = transaction.dataWrite().nodeCreate();

        transaction.dataWrite().nodeAddLabel( node1, foo );
        transaction.dataWrite().nodeSetProperty( node1, name, Values.of( "foo" ) );

        long node2 = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeAddLabel( node2, foo );

        transaction.dataWrite().nodeSetProperty( node2, name, Values.of( "foo" ) );
        commit();

        // when
        LabelSchemaDescriptor descriptor = SchemaDescriptor.forLabel( foo, name );
        var e = assertThrows( CreateConstraintFailureException.class, () ->
        {
            SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
            schemaWriteOperations.uniquePropertyConstraintCreate( descriptor, "constraint name" );
        } );

        assertEquals( ConstraintDescriptorFactory.uniqueForSchema( descriptor ), e.constraint() );
        Throwable cause = e.getCause();
        assertThat( cause, instanceOf( ConstraintValidationException.class ) );
        rollback();

        String expectedMessage = String.format( "Both Node(%d) and Node(%d) have the label `Foo` and property `name` = 'foo'", node1, node2 );
        String actualMessage = userMessage( (ConstraintValidationException) cause );
        assertEquals( expectedMessage, actualMessage );
    }

    @Test
    void shouldCreateAnIndexToGoAlongWithAUniquePropertyConstraint() throws Exception
    {
        // when
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        ConstraintDescriptor constraint = schemaWriteOperations.uniquePropertyConstraintCreate( schema, "constraint name" );
        IndexDescriptor uniqueIndex = kernelTransaction.schemaRead().indexGetForName( constraint.getName() );

        commit();

        // then
        KernelTransaction transaction = newTransaction();
        assertEquals( asSet( uniqueIndex ), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();
    }

    @Test
    void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( LoginContext.AUTH_DISABLED );
        ConstraintDescriptor constraint = transaction.schemaWrite().uniquePropertyConstraintCreate( schema, "constraint name" );
        IndexDescriptor uniqueIndex = kernelTransaction.schemaRead().indexGetForName( constraint.getName() );
        assertEquals( asSet( uniqueIndex ), asSet( transaction.schemaRead().indexesGetAll() ) );

        // when
        rollback();

        // then
        transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();
    }

    @Test
    void shouldNotDropUniquePropertyConstraintThatDoesNotExistWhenThereIsAPropertyExistenceConstraint()
            throws Exception
    {
        // given
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        schemaWriteOperations.nodePropertyExistenceConstraintCreate( schema, "constraint name" );
        commit();

        // when
        var e = assertThrows( DropConstraintFailureException.class, () ->
        {
            try
            {
                SchemaWrite statement = schemaWriteInNewTransaction();
                statement.constraintDrop( ConstraintDescriptorFactory.uniqueForSchema( schema ).withName( "other constraint" ) );
            }
            finally
            {
                rollback();
            }
        } );

        // then
        assertThat( e.getCause(), instanceOf( NoSuchConstraintException.class ) );

        // then
        {
            KernelTransaction transaction = newTransaction();

            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetForSchema( schema );

            assertEquals( ConstraintDescriptorFactory.existsForSchema( schema ), single( constraints ) );
            commit();
        }
    }

    @Test
    void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        SchemaWrite statement = schemaWriteInNewTransaction();
        statement.uniquePropertyConstraintCreate( schema, "constraint name" );
        commit();

        // then
        SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( neoStores().getSchemaStore(), tokenHolders() );
        IndexDescriptor indexRule = ArrayUtil.single( schemaRuleAccess.indexGetForSchema( TestIndexDescriptorFactory
                .uniqueForLabel( typeId, propertyKeyId ) ) );
        ConstraintDescriptor constraintRule = schemaRuleAccess.constraintsGetSingle(
                ConstraintDescriptorFactory.uniqueForLabel( typeId, propertyKeyId ) );
        OptionalLong owningConstraintId = indexRule.getOwningConstraintId();
        assertTrue( owningConstraintId.isPresent() );
        assertEquals( constraintRule.getId(), owningConstraintId.getAsLong() );
        assertEquals( indexRule.getId(), constraintRule.asIndexBackedConstraint().ownedIndexId() );
    }

    @Test
    void shouldIncludeConflictWhenThrowingOnConstraintViolation()
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            tx.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            tx.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            tx.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            tx.commit();
        }

        // when
        var e = assertThrows( Exception.class, () ->
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.uniquePropertyConstraintCreate( schema, "constraint name" );
            commit();
        } );

        // then
        Throwable rootCause = getRootCause( e );
        assertThat( rootCause, instanceOf( IndexEntryConflictException.class ) );
        assertThat( rootCause.getMessage(), stringContainsInOrder( asList( "Both node", "share the property value", "smurf" ) ) );
        assertableLogProvider.rawMessageMatcher().assertContains( stringContainsInOrder( asList( "Failed to populate index:", KEY, PROP ) ) );
    }

    private NeoStores neoStores()
    {
        return db.getDependencyResolver().resolveDependency( RecordStorageEngine.class ).testAccessNeoStores();
    }

    private TokenHolders tokenHolders()
    {
        return db.getDependencyResolver().resolveDependency( TokenHolders.class );
    }

    @Test
    void shouldDropConstraintIndexWhenDroppingConstraint() throws Exception
    {
        // given
        KernelTransaction transaction = newTransaction( LoginContext.AUTH_DISABLED );
        ConstraintDescriptor constraint = transaction.schemaWrite().uniquePropertyConstraintCreate( schema, "constraint name" );
        IndexDescriptor uniqueIndex = kernelTransaction.schemaRead().indexGetForName( constraint.getName() );
        assertEquals( asSet( uniqueIndex ), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();

        // when
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        schemaWriteOperations.constraintDrop( constraint );
        commit();

        // then
        transaction = newTransaction();
        assertEquals( emptySet(), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();
    }

    private String userMessage( ConstraintValidationException cause )
            throws TransactionFailureException
    {
        try ( KernelTransaction tx = newTransaction() )
        {
            TokenNameLookup lookup = new SilentTokenNameLookup( tx.tokenRead() );
            return cause.getUserMessage( lookup );
        }
    }
}
