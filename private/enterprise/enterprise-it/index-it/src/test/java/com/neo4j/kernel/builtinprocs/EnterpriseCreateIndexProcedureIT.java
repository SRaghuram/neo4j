/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.builtinprocs;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;

import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.values.storable.Values.stringOrNoValue;
import static org.neo4j.values.storable.Values.stringValue;

class EnterpriseCreateIndexProcedureIT extends KernelIntegrationTest
{
    private static final GraphDatabaseSettings.SchemaIndex nonDefaultSchemaIndex = NATIVE30;

    private static Stream<Arguments> parameters()
    {
        return Stream.of(
                arguments( false, "createIndex", "index created"),
                arguments( true, "createUniquePropertyConstraint", "uniqueness constraint online"),
                arguments( true, "createNodeKey", "node key constraint online")
        );
    }

    private boolean uniquenessConstraint;
    private String indexProcedureName;
    private String expectedSuccessfulCreationStatus;

    @ParameterizedTest
    @MethodSource( "parameters" )
    void createIndexWithGivenProvider( boolean uniquenessConstraint, String indexProcedureName,
                                       String expectedSuccessfulCreationStatus ) throws KernelException
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        testCreateIndexWithGivenProvider( "Person", "name" );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void createIndexWithGivenProviderComposite( boolean uniquenessConstraint, String indexProcedureName,
                                                String expectedSuccessfulCreationStatus ) throws KernelException
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        testCreateIndexWithGivenProvider( "NinjaTurtle", "favoritePizza", "favoriteBrother" );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void shouldCreateNonExistingLabelAndPropertyToken( boolean uniquenessConstraint, String indexProcedureName,
                                                       String expectedSuccessfulCreationStatus ) throws Exception
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        // given
        String label = "MyLabel";
        String propKey = "myKey";
        KernelTransaction transaction = newTransaction( AnonymousContext.read() );
        assertEquals( TokenRead.NO_TOKEN, transaction.tokenRead().nodeLabel( label ), "label token should not exist" );
        assertEquals( TokenRead.NO_TOKEN, transaction.tokenRead().propertyKey( propKey ), "property token should not exist" );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        callIndexProcedure( indexPattern( label, propKey ), nonDefaultSchemaIndex.providerName() );
        commit();

        // then
        transaction = newTransaction( AnonymousContext.read() );
        assertNotEquals( TokenRead.NO_TOKEN, transaction.tokenRead().nodeLabel( label ), "label token should exist" );
        assertNotEquals( TokenRead.NO_TOKEN, transaction.tokenRead().propertyKey( propKey ), "property token should exist" );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void throwIfNullProvider( boolean uniquenessConstraint, String indexProcedureName, String expectedSuccessfulCreationStatus )
        throws Exception
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        createProperties( transaction, "name" );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( "Person", "name" );
        ProcedureException e = assertThrows( ProcedureException.class, () -> callIndexProcedure( pattern, null ) );

        // then
        assertThat( e.getMessage(), containsString( "Could not create index with specified index provider being null" ) );
        commit();
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void throwIfNonExistingProvider( boolean uniquenessConstraint, String indexProcedureName,
                                     String expectedSuccessfulCreationStatus ) throws Exception
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        transaction.tokenWrite().labelGetOrCreateForName( "Person" );
        createProperties( transaction, "name" );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( "Person", "name" );
        var e = assertThrows( ProcedureException.class, () -> callIndexProcedure( pattern, "non+existing-1.0" ) );
        assertThat( e.getMessage(), allOf(
                containsString( "Failed to invoke procedure" ),
                containsString( "Tried to get index provider" ),
                containsString( "available providers in this session being" ),
                containsString( "default being" )
            ) );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void throwIfIndexAlreadyExists( boolean uniquenessConstraint, String indexProcedureName,
                                    String expectedSuccessfulCreationStatus ) throws Exception
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        // given
        String label = "Superhero";
        String propertyKey = "primaryPower";
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( Label.label( label ) ).on( propertyKey ).create();
            tx.commit();
        }
        awaitIndexOnline();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( label, propertyKey );
        var e = assertThrows( ProcedureException.class, () -> callIndexProcedure( pattern, nonDefaultSchemaIndex.providerName() ) );
        // todo update expected message in accordance with EquivalentSchemaRuleAlreadyExistsException
        assertThat( e.getMessage(), containsString( "" ) );
    }

    private void init( boolean uniquenessConstraint, String indexProcedureName, String expectedSuccessfulCreationStatus )
    {
        this.uniquenessConstraint = uniquenessConstraint;
        this.indexProcedureName = indexProcedureName;
        this.expectedSuccessfulCreationStatus = expectedSuccessfulCreationStatus;
    }

    private static int[] createProperties( KernelTransaction transaction, String... properties ) throws KernelException
    {
        int[] propertyKeyIds = new int[properties.length];
        for ( int i = 0; i < properties.length; i++ )
        {
            propertyKeyIds[i] = transaction.tokenWrite().propertyKeyGetOrCreateForName( properties[i] );
        }
        return propertyKeyIds;
    }

    private static long createNodeWithPropertiesAndLabel( KernelTransaction transaction, int labelId, int[] propertyKeyIds, TextValue value )
            throws KernelException
    {
        long node = transaction.dataWrite().nodeCreate();
        transaction.dataWrite().nodeAddLabel( node, labelId );
        for ( int propertyKeyId : propertyKeyIds )
        {
            transaction.dataWrite().nodeSetProperty( node, propertyKeyId, value );
        }
        return node;
    }

    private static String indexPattern( String label, String... properties )
    {
        StringJoiner pattern = new StringJoiner( ",", ":" + label + "(", ")" );
        for ( String property : properties )
        {
            pattern.add( property );
        }
        return pattern.toString();
    }

    private RawIterator<AnyValue[],ProcedureException> callIndexProcedure( String pattern, String specifiedProvider )
            throws ProcedureException, TransactionFailureException
    {
        Procedures procedures = procsSchema();
        int procedureId = procedures.procedureGet( ProcedureSignature.procedureName( "db", indexProcedureName ) ).id();
        return procedures.procedureCallSchema( procedureId,
                new AnyValue[]
                        {
                                stringOrNoValue( pattern ), // index
                                stringOrNoValue( specifiedProvider ) // providerName
                        },
                ProcedureCallContext.EMPTY );
    }

    private void awaitIndexOnline()
    {
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.SECONDS );
            tx.commit();
        }
    }

    private void testCreateIndexWithGivenProvider( String label, String... properties ) throws KernelException
    {
        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        long node = createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        // when
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( label, properties );
        String specifiedProvider = nonDefaultSchemaIndex.providerName();
        RawIterator<AnyValue[], ProcedureException> result = callIndexProcedure( pattern, specifiedProvider );
        // then
        assertThat( Arrays.asList( result.next() ), contains( stringValue( pattern ), stringValue( specifiedProvider ),
                stringValue( expectedSuccessfulCreationStatus ) ) );
        commit();
        awaitIndexOnline();

        // and then
        transaction = newTransaction( AnonymousContext.read() );
        SchemaRead schemaRead = transaction.schemaRead();
        IndexDescriptor index = schemaRead.index( labelId, propertyKeyIds );
        assertCorrectIndex( labelId, propertyKeyIds, uniquenessConstraint, index );
        assertIndexData( transaction, propertyKeyIds, value, node, index );
        commit();
    }

    private static void assertIndexData( KernelTransaction transaction, int[] propertyKeyIds, TextValue value, long node, IndexDescriptor index )
            throws KernelException
    {
        try ( NodeValueIndexCursor indexCursor = transaction.cursors().allocateNodeValueIndexCursor() )
        {
            IndexQuery[] query = new IndexQuery[propertyKeyIds.length];
            for ( int i = 0; i < propertyKeyIds.length; i++ )
            {
                query[i] = IndexQuery.exact( propertyKeyIds[i], value );
            }
            IndexReadSession indexSession = transaction.dataRead().indexReadSession( index );
            transaction.dataRead().nodeIndexSeek( indexSession, indexCursor, IndexOrder.NONE, false, query );
            assertTrue( indexCursor.next() );
            assertEquals( node, indexCursor.nodeReference() );
            assertFalse( indexCursor.next() );
        }
    }

    private static void assertCorrectIndex( int labelId, int[] propertyKeyIds, boolean expectedUnique, IndexDescriptor index )
    {
        assertEquals( nonDefaultSchemaIndex.providerKey(), index.getIndexProvider().getKey(), "provider key" );
        assertEquals( nonDefaultSchemaIndex.providerVersion(), index.getIndexProvider().getVersion(), "provider version" );
        assertEquals( expectedUnique, index.isUnique() );
        assertEquals( labelId, index.schema().getEntityTokenIds()[0], "label id" );
        for ( int i = 0; i < propertyKeyIds.length; i++ )
        {
            assertEquals( propertyKeyIds[i], index.schema().getPropertyIds()[i], "property key id" );
        }
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void throwOnUniquenessViolation( boolean uniquenessConstraint, String indexProcedureName,
                                     String expectedSuccessfulCreationStatus ) throws Exception
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        testThrowOnUniquenessViolation( "MyLabel", "oneKey" );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void throwOnUniquenessViolationComposite( boolean uniquenessConstraint, String indexProcedureName,
                                              String expectedSuccessfulCreationStatus ) throws Exception
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        testThrowOnUniquenessViolation( "MyLabel", "oneKey", "anotherKey" );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void throwOnNonUniqueStore( boolean uniquenessConstraint, String indexProcedureName, String expectedSuccessfulCreationStatus )
        throws Exception
    {
        init( uniquenessConstraint, indexProcedureName, expectedSuccessfulCreationStatus );
        assumeTrue( uniquenessConstraint, "Only relevant for exuniqueness constraints" );

        // given
        String label = "SomeLabel";
        String[] properties = new String[]{"key1", "key2"};
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        // when
        var e = assertThrows( ProcedureException.class, () -> createConstraint( label, properties ) );
        assertThat( getRootCause( e ), instanceOf( IndexEntryConflictException.class ) );
    }

    @SuppressWarnings( "SameParameterValue" )
    private void testThrowOnUniquenessViolation( String label, String... properties ) throws Exception
    {
        assumeTrue( uniquenessConstraint, "Only relevant for uniqueness constraints" );

        // given
        KernelTransaction transaction = newTransaction( AnonymousContext.writeToken() );
        int labelId = transaction.tokenWrite().labelGetOrCreateForName( label );
        int[] propertyKeyIds = createProperties( transaction, properties );
        TextValue value = stringValue( "some value" );
        createNodeWithPropertiesAndLabel( transaction, labelId, propertyKeyIds, value );
        commit();

        createConstraint( label, properties );

        // when
        assertThrows( UniquePropertyValueValidationException.class, () ->
        {
            var tx = newTransaction( AnonymousContext.write() );
            createNodeWithPropertiesAndLabel( tx, labelId, propertyKeyIds, value );
        } );
    }

    private void createConstraint( String label, String... properties ) throws TransactionFailureException, ProcedureException
    {
        newTransaction( AnonymousContext.full() );
        String pattern = indexPattern( label, properties );
        String specifiedProvider = nonDefaultSchemaIndex.providerName();
        callIndexProcedure( pattern, specifiedProvider );
        commit();
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( File databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }
}
