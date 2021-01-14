/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.builtinprocs;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.Procedures;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.internal.kernel.api.procs.ProcedureSignature;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.exceptions.schema.UniquePropertyValueValidationException;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.impl.api.integrationtest.KernelIntegrationTest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex.NATIVE30;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.values.storable.Values.stringArray;
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
        callIndexProcedure( null, nonDefaultSchemaIndex.providerName(), label, propKey );
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
        ProcedureException e = assertThrows( ProcedureException.class, () -> callIndexProcedure( null, null, "Person", "name" ) );

        // then
        assertThat( e.getMessage() ).contains( "Tried to get index provider with name null whereas available providers in this session being" );
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
        var e = assertThrows( ProcedureException.class, () -> callIndexProcedure( null, "non+existing-1.0", "Person", "name" ) );
        assertThat( e.getMessage() ).contains( "Failed to invoke procedure", "Tried to get index provider", "available providers in this session being",
                "default being" );
    }

    @ParameterizedTest
    @MethodSource( "parameters" )
    void throwIfIndexAlreadyExists( boolean uniquenessConstraint, String indexProcedureName,
                                    String expectedSuccessfulCreationStatus )
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
        var e = assertThrows( ProcedureException.class, () -> callIndexProcedure( null, nonDefaultSchemaIndex.providerName(), label, propertyKey ) );

        if ( uniquenessConstraint )
        {
            final String schemaDescription = "(:Superhero {primaryPower})";
            assertEquals( "There already exists an index " + schemaDescription + ". A constraint cannot be created until the index has been dropped.",
                    e.getMessage() );
        }
        else
        {
            final String indexDescription =
                    "Index( id=1, name='index_6c4daedb', type='GENERAL BTREE', schema=(:Superhero {primaryPower}), indexProvider='native-btree-1.0' )";
            assertEquals( "An equivalent index already exists, '" + indexDescription + "'.", e.getMessage() );
        }
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

    private RawIterator<AnyValue[],ProcedureException> callIndexProcedure( String name, String specifiedProvider, String label, String... properties )
            throws ProcedureException
    {
        final AnyValue[] propertiesAsValues = Arrays.stream( properties ).map( Values::stringValue ).toArray( AnyValue[]::new );
        Procedures procedures = procsSchema();
        int procedureId = procedures.procedureGet( ProcedureSignature.procedureName( "db", indexProcedureName ) ).id();
        return procedures.procedureCallSchema( procedureId,
                new AnyValue[]
                        {
                                stringOrNoValue( name ), // name
                                VirtualValues.list( stringValue( label ) ), // labels
                                VirtualValues.list( propertiesAsValues ), // properties
                                stringOrNoValue( specifiedProvider ), // providerName
                                VirtualValues.map( new String[0], new AnyValue[0] ) // config
                        },
                ProcedureCallContext.EMPTY );
    }

    private void awaitIndexOnline()
    {
        try ( org.neo4j.graphdb.Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 30, TimeUnit.SECONDS );
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
        String name = "MyIndex";
        String specifiedProvider = nonDefaultSchemaIndex.providerName();
        RawIterator<AnyValue[],ProcedureException> result = callIndexProcedure( name, specifiedProvider, label, properties );
        // then
        assertThat( asList( result.next() ) ).containsExactly( stringValue( name ), stringArray( label ), stringArray( properties ),
                stringValue( specifiedProvider ), stringValue( expectedSuccessfulCreationStatus ) );
        commit();
        awaitIndexOnline();

        // and then
        transaction = newTransaction( AnonymousContext.read() );
        SchemaRead schemaRead = transaction.schemaRead();
        IndexDescriptor index = single( schemaRead.index( SchemaDescriptor.forLabel( labelId, propertyKeyIds ) ) );
        assertCorrectIndex( name, labelId, propertyKeyIds, uniquenessConstraint, index );
        assertIndexData( transaction, propertyKeyIds, value, node, index );
        commit();
    }

    private static void assertIndexData( KernelTransaction transaction, int[] propertyKeyIds, TextValue value, long node, IndexDescriptor index )
            throws KernelException
    {
        try ( NodeValueIndexCursor indexCursor = transaction.cursors()
                                                            .allocateNodeValueIndexCursor( transaction.pageCursorTracer(), transaction.memoryTracker() ) )
        {
            PropertyIndexQuery[] query = new PropertyIndexQuery[propertyKeyIds.length];
            for ( int i = 0; i < propertyKeyIds.length; i++ )
            {
                query[i] = PropertyIndexQuery.exact( propertyKeyIds[i], value );
            }
            IndexReadSession indexSession = transaction.dataRead().indexReadSession( index );
            transaction.dataRead().nodeIndexSeek( indexSession, indexCursor, unconstrained(), query );
            assertTrue( indexCursor.next() );
            assertEquals( node, indexCursor.nodeReference() );
            assertFalse( indexCursor.next() );
        }
    }

    private static void assertCorrectIndex( String name, int labelId, int[] propertyKeyIds, boolean expectedUnique, IndexDescriptor index )
    {
        assertEquals( name, index.getName() );
        assertEquals( nonDefaultSchemaIndex.providerName(), index.getIndexProvider().name(), "provider name" );
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
        assumeTrue( uniquenessConstraint, "Only relevant for uniqueness constraints" );

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
        assertThat( getRootCause( e ) ).isInstanceOf( IndexEntryConflictException.class );
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

    private void createConstraint( String label, String... properties ) throws ProcedureException
    {
        newTransaction( AnonymousContext.full() );
        String specifiedProvider = nonDefaultSchemaIndex.providerName();
        callIndexProcedure( null, specifiedProvider, label, properties );
        commit();
    }

    @Override
    protected TestDatabaseManagementServiceBuilder createGraphDatabaseFactory( Path databaseRootDir )
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( databaseRootDir );
    }
}
