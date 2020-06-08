/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise;

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.RelationTypeSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaProcessor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.schema.NodePropertyExistenceException;
import org.neo4j.kernel.api.exceptions.schema.RelationshipPropertyExistenceException;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static org.neo4j.collection.PrimitiveArrays.union;
import static org.neo4j.internal.kernel.api.exceptions.schema.ConstraintValidationException.Phase.VALIDATION;

class PropertyExistenceEnforcer
{
    static PropertyExistenceEnforcer getOrCreatePropertyExistenceEnforcerFrom( StorageReader storageReader )
    {
        return storageReader.getOrCreateSchemaDependantState( PropertyExistenceEnforcer.class, FACTORY );
    }

    private final List<LabelSchemaDescriptor> nodeConstraints;
    private final List<RelationTypeSchemaDescriptor> relationshipConstraints;
    private final MutableLongObjectMap<int[]> mandatoryNodePropertiesByLabel = new LongObjectHashMap<>();
    private final MutableLongObjectMap<int[]> mandatoryRelationshipPropertiesByType = new LongObjectHashMap<>();
    private final TokenNameLookup tokenNameLookup;

    private PropertyExistenceEnforcer( List<LabelSchemaDescriptor> nodes, List<RelationTypeSchemaDescriptor> rels, TokenNameLookup tokenNameLookup )
    {
        this.nodeConstraints = nodes;
        this.relationshipConstraints = rels;
        this.tokenNameLookup = tokenNameLookup;
        for ( LabelSchemaDescriptor constraint : nodes )
        {
            update( mandatoryNodePropertiesByLabel, constraint.getLabelId(),
                    copyAndSortPropertyIds( constraint.getPropertyIds() ) );
        }
        for ( RelationTypeSchemaDescriptor constraint : rels )
        {
            update( mandatoryRelationshipPropertiesByType, constraint.getRelTypeId(),
                    copyAndSortPropertyIds( constraint.getPropertyIds() ) );
        }
    }

    private static void update( MutableLongObjectMap<int[]> map, int key, int[] sortedValues )
    {
        int[] current = map.get( key );
        if ( current != null )
        {
            sortedValues = union( current, sortedValues );
        }
        map.put( key, sortedValues );
    }

    private static int[] copyAndSortPropertyIds( int[] propertyIds )
    {
        int[] values = new int[propertyIds.length];
        System.arraycopy( propertyIds, 0, values, 0, propertyIds.length );
        Arrays.sort( values );
        return values;
    }

    TxStateVisitor decorate( TxStateVisitor visitor, Read read, CursorFactory cursorFactory, PageCursorTracer pageCursorTracer, MemoryTracker memoryTracker )
    {
        return new Decorator( visitor, read, cursorFactory, pageCursorTracer, memoryTracker );
    }

    private static final PropertyExistenceEnforcer NO_CONSTRAINTS = new PropertyExistenceEnforcer(
            emptyList(), emptyList(), null /*not used when there are no constraints*/ )
    {
        @Override
        TxStateVisitor decorate( TxStateVisitor visitor, Read read, CursorFactory cursorFactory, PageCursorTracer pageCursorTracer,
                MemoryTracker memoryTracker )
        {
            return visitor;
        }
    };
    private static final Function<StorageReader,PropertyExistenceEnforcer> FACTORY = storageReader ->
    {
        List<LabelSchemaDescriptor> nodes = new ArrayList<>();
        List<RelationTypeSchemaDescriptor> relationships = new ArrayList<>();
        for ( Iterator<ConstraintDescriptor> constraints = storageReader.constraintsGetAll(); constraints.hasNext(); )
        {
            ConstraintDescriptor constraint = constraints.next();
            if ( constraint.enforcesPropertyExistence() )
            {
                constraint.schema().processWith( new SchemaProcessor()
                {
                    @Override
                    public void processSpecific( LabelSchemaDescriptor schema )
                    {
                        nodes.add( schema );
                    }

                    @Override
                    public void processSpecific( RelationTypeSchemaDescriptor schema )
                    {
                        relationships.add( schema );
                    }

                    @Override
                    public void processSpecific( SchemaDescriptor schema )
                    {
                        throw new UnsupportedOperationException( "General SchemaDescriptor cannot support constraints" );
                    }
                } );
            }
        }
        if ( nodes.isEmpty() && relationships.isEmpty() )
        {
            return NO_CONSTRAINTS;
        }
        return new PropertyExistenceEnforcer( nodes, relationships, storageReader.tokenNameLookup() );
    };

    private class Decorator extends TxStateVisitor.Delegator
    {
        private final MutableIntSet propertyKeyIds = new IntHashSet();
        private final Read read;
        private final NodeCursor nodeCursor;
        private final PropertyCursor propertyCursor;
        private final RelationshipScanCursor relationshipCursor;

        Decorator( TxStateVisitor next, Read read, CursorFactory cursorFactory, PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
        {
            super( next );
            this.read = read;
            this.nodeCursor = cursorFactory.allocateFullAccessNodeCursor( cursorTracer );
            this.propertyCursor = cursorFactory.allocateFullAccessPropertyCursor( cursorTracer, memoryTracker );
            this.relationshipCursor = cursorFactory.allocateRelationshipScanCursor( cursorTracer );
        }

        @Override
        public void visitNodePropertyChanges(
                long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed,
                IntIterable removed ) throws ConstraintValidationException
        {
            validateNode( id );
            super.visitNodePropertyChanges( id, added, changed, removed );
        }

        @Override
        public void visitNodeLabelChanges( long id, LongSet added, LongSet removed )
                throws ConstraintValidationException
        {
            validateNode( id );
            super.visitNodeLabelChanges( id, added, removed );
        }

        @Override
        public void visitCreatedRelationship( long id, int type, long startNode, long endNode, Iterable<StorageProperty> addedProperties )
                throws ConstraintValidationException
        {
            validateRelationship( id );
            super.visitCreatedRelationship( id, type, startNode, endNode, addedProperties );
        }

        @Override
        public void visitRelPropertyChanges(
<<<<<<< HEAD
                long id, Iterable<StorageProperty> added, Iterable<StorageProperty> changed,
=======
                long id, int type, long startNode, long endNode, Iterable<StorageProperty> added, Iterable<StorageProperty> changed,
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                IntIterable removed ) throws ConstraintValidationException
        {
            validateRelationship( id );
            super.visitRelPropertyChanges( id, type, startNode, endNode, added, changed, removed );
        }

        @Override
        public void close() throws KernelException
        {
            super.close();
            IOUtils.closeAllUnchecked( nodeCursor, relationshipCursor, propertyCursor );
        }

        private void validateNode( long nodeId ) throws NodePropertyExistenceException
        {
            if ( mandatoryNodePropertiesByLabel.isEmpty() )
            {
                return;
            }

            final TokenSet labelIds;
            read.singleNode( nodeId, nodeCursor );
            if ( nodeCursor.next() )
            {
                labelIds = nodeCursor.labels();
                if ( labelIds.numberOfTokens() == 0 )
                {
                    return;
                }
                propertyKeyIds.clear();
                nodeCursor.properties( propertyCursor );
                while ( propertyCursor.next() )
                {
                    propertyKeyIds.add( propertyCursor.propertyKey() );
                }
            }
            else
            {
                throw new IllegalStateException( format( "Node %d with changes should exist.", nodeId ) );
            }

            validateNodeProperties( nodeId, labelIds, propertyKeyIds );
        }

        private void validateRelationship( long id ) throws RelationshipPropertyExistenceException
        {
            if ( mandatoryRelationshipPropertiesByType.isEmpty() )
            {
                return;
            }

            int relationshipType;
            int[] required;
            read.singleRelationship( id, relationshipCursor );
            if ( relationshipCursor.next() )
            {
                relationshipType = relationshipCursor.type();
                required = mandatoryRelationshipPropertiesByType.get( relationshipType );
                if ( required == null )
                {
                    return;
                }
                propertyKeyIds.clear();
                relationshipCursor.properties( propertyCursor );
                while ( propertyCursor.next() )
                {
                    propertyKeyIds.add( propertyCursor.propertyKey() );
                }
            }
            else
            {
                throw new IllegalStateException( format( "Relationship %d with changes should exist.", id ) );
            }

            for ( int mandatory : required )
            {
                if ( !propertyKeyIds.contains( mandatory ) )
                {
                    failRelationship( id, relationshipType, mandatory );
                }
            }
        }
    }

    private void validateNodeProperties( long id, TokenSet labelIds, IntSet propertyKeyIds )
            throws NodePropertyExistenceException
    {
        int numberOfLabels = labelIds.numberOfTokens();
        if ( numberOfLabels > mandatoryNodePropertiesByLabel.size() )
        {
            for ( MutableLongIterator labels = mandatoryNodePropertiesByLabel.keySet().longIterator(); labels.hasNext(); )
            {
                final long label = labels.next();
                if ( labelIds.contains( toIntExact( label ) ) )
                {
                    validateNodeProperties( id, label, mandatoryNodePropertiesByLabel.get( label ), propertyKeyIds );
                }
            }
        }
        else
        {
            for ( int i = 0; i < numberOfLabels; i++ )
            {
                final long label = labelIds.token( i );
                int[] keys = mandatoryNodePropertiesByLabel.get( label );
                if ( keys != null )
                {
                    validateNodeProperties( id, label, keys, propertyKeyIds );
                }
            }
        }
    }

    private void validateNodeProperties( long id, long label, int[] requiredKeys, IntSet propertyKeyIds )
            throws NodePropertyExistenceException
    {
        for ( int key : requiredKeys )
        {
            if ( !propertyKeyIds.contains( key ) )
            {
                failNode( id, label, key );
            }
        }
    }

    private void failNode( long id, long label, int propertyKey )
            throws NodePropertyExistenceException
    {
        for ( LabelSchemaDescriptor constraint : nodeConstraints )
        {
            if ( constraint.getLabelId() == label && contains( constraint.getPropertyIds(), propertyKey ) )
            {
                throw new NodePropertyExistenceException( constraint, VALIDATION, id, tokenNameLookup );
            }
        }
        throw new IllegalStateException( format(
                "Node constraint for label=%d, propertyKey=%d should exist.",
                label, propertyKey ) );
    }

    private void failRelationship( long id, int relationshipType, int propertyKey )
            throws RelationshipPropertyExistenceException
    {
        for ( RelationTypeSchemaDescriptor constraint : relationshipConstraints )
        {
            if ( constraint.getRelTypeId() == relationshipType && contains( constraint.getPropertyIds(), propertyKey ) )
            {
                throw new RelationshipPropertyExistenceException( constraint, VALIDATION, id, tokenNameLookup );
            }
        }
        throw new IllegalStateException( format(
                "Relationship constraint for relationshipType=%d, propertyKey=%d should exist.",
                relationshipType, propertyKey ) );
    }

    private static boolean contains( int[] list, int value )
    {
        for ( int x : list )
        {
            if ( value == x )
            {
                return true;
            }
        }
        return false;
    }
}
