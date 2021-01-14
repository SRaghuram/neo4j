/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.index;

import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimitWithLowerInternalRepresentationThresholdsSmallFactory;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.NodeValueIndexCursor;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.RelationshipValueIndexCursor;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.single;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( RandomExtension.class )
class HighLimitIndexIT
{
    private static final Label LABEL = Label.label( "Label" );
    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName( "TYPE" );
    private static final String KEY = "key";

    @Inject
    private RandomRule random;

    private DatabaseManagementService dbms;
    private GraphDatabaseAPI db;
    private int labelId;
    private int relationshipTypeId;
    private int propertyKeyId;
    // These are mutable both in the sense that they are added to by the "setup" of the test and removed from when verifying
    private final MutableLongSet nodeIds = LongSets.mutable.empty();
    private final MutableLongSet relationshipIds = LongSets.mutable.empty();

    void startAndCreateBasicData( String recordFormat )
    {
        dbms = new TestDatabaseManagementServiceBuilder( Paths.get( "myhome" ) )
                .impermanent()
                .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                .build();
        db = (GraphDatabaseAPI) dbms.database( DEFAULT_DATABASE_NAME );
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            int numNodes = HighLimitWithLowerInternalRepresentationThresholdsSmallFactory.FIXED_REFERENCE_THRESHOLD * 2;
            Node[] nodes = new Node[numNodes];
            for ( int i = 0; i < numNodes; i++ )
            {
                nodes[i] = tx.createNode( LABEL );
                nodes[i].setProperty( KEY, i );
                nodeIds.add( nodes[i].getId() );
            }
            int numRelationships = numNodes * 10;
            for ( int i = 0; i < numRelationships; i++ )
            {
                Relationship relationship = random.among( nodes ).createRelationshipTo( random.among( nodes ), RELATIONSHIP_TYPE );
                relationship.setProperty( KEY, i );
                relationshipIds.add( relationship.getId() );
            }
            labelId = tx.kernelTransaction().tokenRead().nodeLabel( LABEL.name() );
            relationshipTypeId = tx.kernelTransaction().tokenRead().relationshipType( RELATIONSHIP_TYPE.name() );
            propertyKeyId = tx.kernelTransaction().tokenRead().propertyKey( KEY );
            tx.commit();
        }
    }

    @AfterEach
    void shutdown()
    {
        dbms.shutdown();
    }

    @MethodSource( "formats" )
    @ParameterizedTest
    void shouldPopulateNodeIndexOnHighLimitFormat( String recordFormat ) throws KernelException
    {
        // given
        startAndCreateBasicData( recordFormat );

        // when
        SchemaDescriptor schemaDescriptor = SchemaDescriptor.forLabel( labelId, propertyKeyId );
        createIndex( schemaDescriptor );

        // then
        verifyIndex( schemaDescriptor, ( descriptor, session, tx ) ->
        {
            try ( NodeValueIndexCursor cursor = tx.cursors().allocateNodeValueIndexCursor( NULL, INSTANCE ) )
            {
                tx.dataRead().nodeIndexSeek( session, cursor, unconstrained(), PropertyIndexQuery.exists( propertyKeyId ) );
                while ( cursor.next() )
                {
                    assertThat( nodeIds.remove( cursor.nodeReference() ) ).isTrue();
                }
                assertThat( nodeIds.isEmpty() ).isTrue();
            }
        } );
    }

    @MethodSource( "formats" )
    @ParameterizedTest
    void shouldPopulateRelationshipIndexOnHighLimitFormat( String recordFormat ) throws KernelException
    {
        // given
        startAndCreateBasicData( recordFormat );

        // when
        SchemaDescriptor schemaDescriptor = SchemaDescriptor.forRelType( relationshipTypeId, propertyKeyId );
        createIndex( schemaDescriptor );

        // then
        verifyIndex( schemaDescriptor, ( descriptor, session, tx ) ->
        {
            try ( RelationshipValueIndexCursor cursor = tx.cursors().allocateRelationshipValueIndexCursor( NULL, INSTANCE ) )
            {
                tx.dataRead().relationshipIndexSeek( session, cursor, unconstrained(), PropertyIndexQuery.exists( propertyKeyId ) );
                while ( cursor.next() )
                {
                    assertThat( relationshipIds.remove( cursor.relationshipReference() ) ).isTrue();
                }
                assertThat( relationshipIds.isEmpty() ).isTrue();
            }
        } );
    }

    private <C extends Cursor> void verifyIndex( SchemaDescriptor schemaDescriptor, Verifier verifier ) throws KernelException
    {
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            KernelTransaction ktx = tx.kernelTransaction();
            IndexDescriptor descriptor = single( ktx.schemaRead().index( schemaDescriptor ) );
            IndexReadSession session = ktx.dataRead().indexReadSession( descriptor );
            verifier.verify( descriptor, session, ktx );
            tx.commit();
        }
    }

    private void createIndex( SchemaDescriptor schemaDescriptor ) throws KernelException
    {
        try ( InternalTransaction tx = db.beginTransaction( KernelTransaction.Type.EXPLICIT, LoginContext.AUTH_DISABLED ) )
        {
            tx.kernelTransaction().schemaWrite().indexCreate( schemaDescriptor, "TestIndex" );
            tx.commit();
        }
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 10, TimeUnit.MINUTES );
            tx.commit();
        }
    }

    private static Stream<Arguments> formats()
    {
        return Stream.of(
                arguments( HighLimit.NAME ),
                arguments( HighLimitWithLowerInternalRepresentationThresholdsSmallFactory.NAME ) );
    }

    private interface Verifier
    {
        void verify( IndexDescriptor descriptor, IndexReadSession session, KernelTransaction tx ) throws KernelException;
    }
}
