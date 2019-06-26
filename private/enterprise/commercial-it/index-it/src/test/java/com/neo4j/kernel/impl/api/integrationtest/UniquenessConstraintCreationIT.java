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
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.kernel.api.exceptions.schema.CreateConstraintFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor2;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.ConstraintDescriptorFactory;
import org.neo4j.internal.schema.constraints.UniquenessConstraintDescriptor;
import org.neo4j.kernel.api.SilentTokenNameLookup;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.DropConstraintFailureException;
import org.neo4j.kernel.api.exceptions.schema.NoSuchConstraintException;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.storageengine.api.ConstraintRule;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.Values;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
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
    private IndexDescriptor2 uniqueIndex;

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
        return writeOps.uniquePropertyConstraintCreate( descriptor );
    }

    @Override
    void createConstraintInRunningTx( GraphDatabaseService db, String type, String property )
    {
        SchemaHelper.createUniquenessConstraint( db, type, property );
    }

    @Override
    UniquenessConstraintDescriptor newConstraintObject( LabelSchemaDescriptor descriptor )
    {
        return ConstraintDescriptorFactory.uniqueForSchema( descriptor );
    }

    @Override
    void dropConstraint( SchemaWrite writeOps, ConstraintDescriptor constraint ) throws Exception
    {
        writeOps.constraintDrop( constraint );
    }

    @Override
    void createOffendingDataInRunningTx( GraphDatabaseService db )
    {
        db.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
        db.createNode( label( KEY ) ).setProperty( PROP, DUPLICATED_VALUE );
    }

    @Override
    void removeOffendingDataInRunningTx( GraphDatabaseService db )
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( label( KEY ), PROP, DUPLICATED_VALUE ) )
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
        uniqueIndex = TestIndexDescriptorFactory.uniqueForLabel( typeId, propertyKeyId );
        return SchemaDescriptor.forLabel( typeId, propertyKeyId );
    }

    @Test
    void shouldAbortConstraintCreationWhenDuplicatesExist() throws Exception
    {
        // given
        Transaction transaction = newTransaction( AnonymousContext.writeToken() );
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
            schemaWriteOperations.uniquePropertyConstraintCreate( descriptor );
        } );

        assertEquals( ConstraintDescriptorFactory.uniqueForSchema( descriptor ), e.constraint() );
        Throwable cause = e.getCause();
        assertThat( cause, instanceOf( ConstraintValidationException.class ) );

        String expectedMessage = String.format( "Both Node(%d) and Node(%d) have the label `Foo` and property `name` = 'foo'", node1, node2 );
        String actualMessage = userMessage( (ConstraintValidationException) cause );
        assertEquals( expectedMessage, actualMessage );
    }

    @Test
    void shouldCreateAnIndexToGoAlongWithAUniquePropertyConstraint() throws Exception
    {
        // when
        SchemaWrite schemaWriteOperations = schemaWriteInNewTransaction();
        schemaWriteOperations.uniquePropertyConstraintCreate( descriptor );
        commit();

        // then
        Transaction transaction = newTransaction();
        assertEquals( asSet( uniqueIndex ), asSet( transaction.schemaRead().indexesGetAll() ) );
        commit();
    }

    @Test
    void shouldDropCreatedConstraintIndexWhenRollingBackConstraintCreation() throws Exception
    {
        // given
        Transaction transaction = newTransaction( LoginContext.AUTH_DISABLED );
        transaction.schemaWrite().uniquePropertyConstraintCreate( descriptor );
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
        schemaWriteOperations.nodePropertyExistenceConstraintCreate( descriptor );
        commit();

        // when
        var e = assertThrows( DropConstraintFailureException.class, () ->
        {
            try
            {
                SchemaWrite statement = schemaWriteInNewTransaction();
                statement.constraintDrop( ConstraintDescriptorFactory.uniqueForSchema( descriptor ) );
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
            Transaction transaction = newTransaction();

            Iterator<ConstraintDescriptor> constraints = transaction.schemaRead().constraintsGetForSchema( descriptor );

            assertEquals( ConstraintDescriptorFactory.existsForSchema( descriptor ), single( constraints ) );
            commit();
        }
    }

    @Test
    void committedConstraintRuleShouldCrossReferenceTheCorrespondingIndexRule() throws Exception
    {
        // when
        SchemaWrite statement = schemaWriteInNewTransaction();
        statement.uniquePropertyConstraintCreate( descriptor );
        commit();

        // then
        SchemaRuleAccess schemaRuleAccess = SchemaRuleAccess.getSchemaRuleAccess( neoStores().getSchemaStore(), tokenHolders() );
        IndexDescriptor2 indexRule = ArrayUtil.single( schemaRuleAccess.indexGetForSchema( TestIndexDescriptorFactory
                .uniqueForLabel( typeId, propertyKeyId ) ) );
        ConstraintRule constraintRule = schemaRuleAccess.constraintsGetSingle(
                ConstraintDescriptorFactory.uniqueForLabel( typeId, propertyKeyId ) );
        OptionalLong owningConstraintId = indexRule.getOwningConstraintId();
        assertTrue( owningConstraintId.isPresent() );
        assertEquals( constraintRule.getId(), owningConstraintId.getAsLong() );
        assertEquals( indexRule.getId(), constraintRule.ownedIndexReference() );
    }

    @Test
    void shouldIncludeConflictWhenThrowingOnConstraintViolation()
    {
        // given
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            db.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            db.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            db.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            db.createNode( label( KEY ) ).setProperty( PROP, "smurf" );
            tx.success();
        }

        // when
        var e = assertThrows( Exception.class, () ->
        {
            SchemaWrite statement = schemaWriteInNewTransaction();
            statement.uniquePropertyConstraintCreate( descriptor );
            commit();
        } );

        // then
        Throwable rootCause = Exceptions.rootCause( e );
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
        Transaction transaction = newTransaction( LoginContext.AUTH_DISABLED );
        ConstraintDescriptor constraint =
                transaction.schemaWrite().uniquePropertyConstraintCreate( descriptor );
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
        try ( Transaction tx = newTransaction() )
        {
            TokenNameLookup lookup = new SilentTokenNameLookup( tx.tokenRead() );
            return cause.getUserMessage( lookup );
        }
    }
}
