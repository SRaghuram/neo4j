/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexReadSession;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.values.storable.Values;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;
import static org.neo4j.internal.schema.SchemaDescriptor.forRelType;

@ImpermanentDbmsExtension
@Timeout( 20 )
class CompositeIndexingIT
{
    private static final int LABEL_ID = 1;
    private static final int REL_TYPE_ID = 1;

    @Inject
    private GraphDatabaseAPI graphDatabaseAPI;
    private IndexDescriptor index;

    @BeforeEach
    void setup() throws Exception
    {
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            TokenWrite tokenWrite = ktx.tokenWrite();
            tokenWrite.labelGetOrCreateForName( "Label0" );
            assertEquals( LABEL_ID, tokenWrite.labelGetOrCreateForName( "Label1" ) );
            tokenWrite.relationshipTypeGetOrCreateForName( "Type0" );
            assertEquals( REL_TYPE_ID, tokenWrite.relationshipTypeGetOrCreateForName( "Type1" ) );
            for ( int i = 0; i < 10; i++ )
            {
                tokenWrite.propertyKeyGetOrCreateForName( "prop" + i );
            }
            tx.commit();
        }
    }

    void setup( IndexPrototype prototype ) throws Exception
    {
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            if ( prototype.isUnique() )
            {
                ConstraintDescriptor constraint = ktx.schemaWrite().uniquePropertyConstraintCreate( prototype );
                index = ktx.schemaRead().indexGetForName( constraint.getName() );
            }
            else
            {
                index = ktx.schemaWrite().indexCreate( prototype.schema(), null );
            }
            tx.commit();
        }

        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            tx.schema().awaitIndexesOnline( 5, MINUTES );
            tx.commit();
        }
    }

    @AfterEach
    void clean() throws Exception
    {
        if ( index == null )
        {
            // setup no happened
            return;
        }
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            if ( index.isUnique() )
            {
                Iterator<ConstraintDescriptor> constraints = ktx.schemaRead().constraintsGetForSchema( index.schema() );
                while ( constraints.hasNext() )
                {
                    ktx.schemaWrite().constraintDrop( constraints.next() );
                }
            }
            else
            {
                ktx.schemaWrite().indexDrop( index );
            }
            tx.commit();
        }

        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            for ( Node node : tx.getAllNodes() )
            {
                node.getRelationships().forEach( Relationship::delete );
                node.delete();
            }
            tx.commit();
        }
    }

    private static Stream<Arguments> params()
    {
        return Stream.of(
                arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1 ) ), EntityControl.NODE ),
                arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1, 2 ) ), EntityControl.NODE ),
                arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1, 2, 3, 4 ) ), EntityControl.NODE ),
                arguments( IndexPrototype.forSchema( forLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) ), EntityControl.NODE ),
                arguments( IndexPrototype.uniqueForSchema( forLabel( LABEL_ID, 1 ) ), EntityControl.NODE ),
                arguments( IndexPrototype.uniqueForSchema( forLabel( LABEL_ID, 1, 2 ) ), EntityControl.NODE ),
                arguments( IndexPrototype.uniqueForSchema( forLabel( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) ), EntityControl.NODE ),
                arguments( IndexPrototype.forSchema( forRelType( LABEL_ID, 1 ) ), EntityControl.RELATIONSHIP ),
                arguments( IndexPrototype.forSchema( forRelType( LABEL_ID, 1, 2 ) ), EntityControl.RELATIONSHIP ),
                arguments( IndexPrototype.forSchema( forRelType( LABEL_ID, 1, 2, 3, 4 ) ), EntityControl.RELATIONSHIP ),
                arguments( IndexPrototype.forSchema( forRelType( LABEL_ID, 1, 2, 3, 4, 5, 6, 7 ) ), EntityControl.RELATIONSHIP )
        );
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeEntityAddedByPropertyToIndexInTranslation( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        setup( prototype );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            var entity = entityControl.createEntity( ktx, index );

            var found = entityControl.seek( ktx, index );
            assertThat( found ).containsExactly( entity );
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeEntityAddedByTokenToIndexInTransaction( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        assumeTrue( entityControl == EntityControl.NODE );
        setup( prototype );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            var entity = entityControl.createEntityReverse( ktx, index );

            var found = entityControl.seek( ktx, index );
            assertThat( found ).containsExactly( entity );
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeEntityThatWasDeletedInTransaction( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        setup( prototype );
        long nodeID = createEntity( entityControl );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.deleteEntity( ktx, nodeID );

            assertThat( entityControl.seek( ktx, index ) ).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeEntityThatHasItsTokenRemovedInTransaction( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        assumeTrue( entityControl == EntityControl.NODE );
        setup( prototype );
        long nodeID = createEntity( entityControl );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.removeToken( ktx, nodeID );

            assertThat( entityControl.seek( ktx, index ) ).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeEntityhatHasAPropertyRemovedInTransaction( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        setup( prototype );
        long entity = createEntity( entityControl );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.removeProperty( ktx, entity, index.schema().getPropertyIds()[0] );

            assertThat( entityControl.seek( ktx, index ) ).isEmpty();
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeAllEntitiesAddedInTransaction( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        setup( prototype );
        if ( !index.isUnique() ) // this test does not make any sense for UNIQUE indexes
        {
            try ( Transaction tx = graphDatabaseAPI.beginTx() )
            {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

                long entity1 = entityControl.createEntity( ktx, index );
                long entity2 = entityControl.createEntity( ktx, index );
                long entity3 = entityControl.createEntity( ktx, index );

                assertThat( entityControl.seek( ktx, index ) ).contains( entity1, entity2, entity3 );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldSeeAllEntitiesAddedBeforeTransaction( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        setup( prototype );
        if ( !index.isUnique() ) // this test does not make any sense for UNIQUE indexes
        {
            long entity1 = createEntity( entityControl );
            long entity2 = createEntity( entityControl );
            long entity3 = createEntity( entityControl );
            try ( Transaction tx = graphDatabaseAPI.beginTx() )
            {
                KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();

                assertThat( entityControl.seek( ktx, index ) ).contains( entity1, entity2, entity3 );
            }
        }
    }

    @ParameterizedTest
    @MethodSource( "params" )
    void shouldNotSeeEntitiesLackingOneProperty( IndexPrototype prototype, EntityControl entityControl ) throws Exception
    {
        setup( prototype );
        long entity = createEntity( entityControl );
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            entityControl.createEntity( ktx, index, true );

            assertThat( entityControl.seek( ktx, index ) ).containsExactly( entity );
        }
    }

    private long createEntity( EntityControl entityControl )
            throws KernelException
    {
        long id;
        try ( Transaction tx = graphDatabaseAPI.beginTx() )
        {
            KernelTransaction ktx = ((InternalTransaction) tx).kernelTransaction();
            id = entityControl.createEntity( ktx, index );
            tx.commit();
        }
        return id;
    }

    private static PropertyIndexQuery[] exactQuery( IndexDescriptor index )
    {
        int[] propertyIds = index.schema().getPropertyIds();
        PropertyIndexQuery[] query = new PropertyIndexQuery[propertyIds.length];
        for ( int i = 0; i < query.length; i++ )
        {
            int propID = propertyIds[i];
            query[i] = PropertyIndexQuery.exact( propID, Values.of( propID ) );
        }
        return query;
    }

    enum EntityControl
    {
        NODE
                {
                    @Override
                    long createEntity( KernelTransaction ktx, IndexDescriptor index, boolean excludeFirstProperty ) throws KernelException
                    {
                        Write write = ktx.dataWrite();
                        var nodeID = write.nodeCreate();
                        write.nodeAddLabel( nodeID, LABEL_ID );
                        for ( int propID : index.schema().getPropertyIds() )
                        {
                            if ( excludeFirstProperty )
                            {
                                excludeFirstProperty = false;
                                continue;
                            }
                            write.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
                        }
                        return nodeID;
                    }

                    @Override
                    long createEntityReverse( KernelTransaction ktx, IndexDescriptor index ) throws KernelException
                    {
                        Write write = ktx.dataWrite();
                        var nodeID = write.nodeCreate();
                        for ( int propID : index.schema().getPropertyIds() )
                        {
                            write.nodeSetProperty( nodeID, propID, Values.intValue( propID ) );
                        }
                        write.nodeAddLabel( nodeID, LABEL_ID );
                        return nodeID;
                    }

                    @Override
                    public void deleteEntity( KernelTransaction ktx, long id ) throws InvalidTransactionTypeKernelException
                    {
                        ktx.dataWrite().nodeDelete( id );
                    }

                    @Override
                    public void removeToken( KernelTransaction ktx, long entityId ) throws InvalidTransactionTypeKernelException, EntityNotFoundException
                    {
                        ktx.dataWrite().nodeRemoveLabel( entityId, LABEL_ID );
                    }

                    @Override
                    public void removeProperty( KernelTransaction ktx, long entity, int propertyId ) throws KernelException
                    {
                        ktx.dataWrite().nodeRemoveProperty( entity, propertyId );
                    }

                    @Override
                    Set<Long> seek( KernelTransaction ktx, IndexDescriptor index ) throws KernelException
                    {
                        IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
                        Set<Long> result = new HashSet<>();
                        try ( var cursor = ktx.cursors().allocateNodeValueIndexCursor( ktx.pageCursorTracer(), ktx.memoryTracker() ) )
                        {
                            ktx.dataRead().nodeIndexSeek( indexSession, cursor, unconstrained(), exactQuery( index ) );
                            while ( cursor.next() )
                            {
                                result.add( cursor.nodeReference() );
                            }
                        }
                        return result;
                    }
                },
        RELATIONSHIP
                {
                    @Override
                    long createEntity( KernelTransaction ktx, IndexDescriptor index, boolean excludeFirstProperty ) throws KernelException
                    {
                        Write write = ktx.dataWrite();
                        var from = write.nodeCreate();
                        var to = write.nodeCreate();
                        var rel = write.relationshipCreate( from, REL_TYPE_ID, to );
                        for ( int propID : index.schema().getPropertyIds() )
                        {
                            if ( excludeFirstProperty )
                            {
                                excludeFirstProperty = false;
                                continue;
                            }
                            write.relationshipSetProperty( rel, propID, Values.intValue( propID ) );
                        }
                        return rel;
                    }

                    @Override
                    long createEntityReverse( KernelTransaction ktx, IndexDescriptor index ) throws KernelException
                    {
                        return createEntity( ktx, index, false );
                    }

                    @Override
                    public void deleteEntity( KernelTransaction ktx, long id ) throws InvalidTransactionTypeKernelException
                    {
                        ktx.dataWrite().relationshipDelete( id );
                    }

                    @Override
                    public void removeToken( KernelTransaction ktx, long entityId )
                    {
                        throw new IllegalStateException( "Not supported" );
                    }

                    @Override
                    public void removeProperty( KernelTransaction ktx, long entity, int propertyId ) throws KernelException
                    {
                        ktx.dataWrite().relationshipRemoveProperty( entity, propertyId );
                    }

                    @Override
                    Set<Long> seek( KernelTransaction ktx, IndexDescriptor index ) throws KernelException
                    {
                        IndexReadSession indexSession = ktx.dataRead().indexReadSession( index );
                        Set<Long> result = new HashSet<>();
                        try ( var cursor = ktx.cursors().allocateRelationshipValueIndexCursor( ktx.pageCursorTracer(), ktx.memoryTracker() ) )
                        {
                            ktx.dataRead().relationshipIndexSeek( indexSession, cursor, unconstrained(), exactQuery( index ) );
                            while ( cursor.next() )
                            {
                                result.add( cursor.relationshipReference() );
                            }
                        }
                        return result;
                    }
                };

        long createEntity( KernelTransaction ktx, IndexDescriptor index ) throws KernelException
        {
            return createEntity( ktx, index, false );
        }

        abstract long createEntity( KernelTransaction ktx, IndexDescriptor index, boolean excludeFirstProperty ) throws KernelException;

        abstract long createEntityReverse( KernelTransaction ktx, IndexDescriptor index ) throws KernelException;

        abstract Set<Long> seek( KernelTransaction transaction, IndexDescriptor index ) throws KernelException;

        public abstract void deleteEntity( KernelTransaction ktx, long id ) throws KernelException;

        public abstract void removeToken( KernelTransaction ktx, long entityId ) throws KernelException;

        public abstract void removeProperty( KernelTransaction ktx, long entity, int propertyId ) throws KernelException;
    }
}
