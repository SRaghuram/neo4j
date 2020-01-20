/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.freki;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.Function;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.counts.GBPTreeCountsStore;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaCache;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.storageengine.api.AllNodeScan;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipGroupCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.token.TokenHolders;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

class FrekiStorageReader implements StorageReader
{
    private final Stores stores;
    private final SchemaCache schemaCache;
    private final GBPTreeCountsStore counts;
    private final TokenHolders tokenHolders;

    FrekiStorageReader( Stores stores, TokenHolders tokenHolders )
    {
        this.stores = stores;
        this.schemaCache = stores.schemaCache;
        this.counts = stores.countsStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public void close()
    {
    }

    @Override
    public Iterator<IndexDescriptor> indexGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.indexesForSchema( descriptor );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( int labelId )
    {
        return schemaCache.indexesForLabel( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType( int relationshipType )
    {
        return schemaCache.indexesForRelationshipType( relationshipType );
    }

    @Override
    public IndexDescriptor indexGetForName( String name )
    {
        return schemaCache.indexForName( name );
    }

    @Override
    public ConstraintDescriptor constraintGetForName( String name )
    {
        return schemaCache.constraintForName( name );
    }

    @Override
    public boolean indexExists( IndexDescriptor index )
    {
        return schemaCache.hasIndex( index );
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        return schemaCache.indexes().iterator();
    }

    @Override
    public Collection<IndexDescriptor> indexesGetRelated( long[] labels, int propertyKeyId, EntityType entityType )
    {
        return schemaCache.getIndexesRelatedTo( EMPTY_LONG_ARRAY, labels, new int[]{propertyKeyId}, false, entityType );
    }

    @Override
    public Collection<IndexDescriptor> indexesGetRelated( long[] labels, int[] propertyKeyIds, EntityType entityType )
    {
        return schemaCache.getIndexesRelatedTo( labels, PrimitiveLongCollections.EMPTY_LONG_ARRAY, propertyKeyIds, true, entityType );
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated( long[] labels, int propertyKeyId, EntityType entityType )
    {
        return schemaCache.getUniquenessConstraintsRelatedTo( PrimitiveLongCollections.EMPTY_LONG_ARRAY, labels, new int[] {propertyKeyId}, false, entityType );
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated( long[] labels, int[] propertyKeyIds, EntityType entityType )
    {
        return schemaCache.getUniquenessConstraintsRelatedTo( labels, PrimitiveLongCollections.EMPTY_LONG_ARRAY, propertyKeyIds, true, entityType );
    }

    @Override
    public boolean hasRelatedSchema( long[] labels, int propertyKey, EntityType entityType )
    {
        return schemaCache.hasRelatedSchema( labels, propertyKey, entityType );
    }

    @Override
    public boolean hasRelatedSchema( int label, EntityType entityType )
    {
        return schemaCache.hasRelatedSchema( label, entityType );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForSchema( SchemaDescriptor descriptor )
    {
        return schemaCache.constraintsForSchema( descriptor );
    }

    @Override
    public boolean constraintExists( ConstraintDescriptor descriptor )
    {
        return schemaCache.hasConstraintRule( descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        return schemaCache.constraintsForLabel( labelId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        return schemaCache.constraintsForRelationshipType( typeId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        return schemaCache.constraints().iterator();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        if ( index == null )
        {
            return null;
        }
        OptionalLong owningConstraintId = index.getOwningConstraintId();
        if ( owningConstraintId.isPresent() )
        {
            Long constraintId = owningConstraintId.getAsLong();
            if ( schemaCache.hasConstraintRule( constraintId ) )
            {
                return constraintId;
            }
        }
        return null;
    }

    @Override
    public long countsForNode( int labelId )
    {
        return counts.nodeCount( labelId );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        if ( !(startLabelId == ANY_LABEL || endLabelId == ANY_LABEL) )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
        return counts.relationshipCount( startLabelId, typeId, endLabelId );
    }

    @Override
    public long nodesGetCount()
    {
        return counts.nodeCount( ANY_LABEL );
    }

    @Override
    public long relationshipsGetCount()
    {
        // TODO
        return 0;
    }

    @Override
    public int labelCount()
    {
        return tokenHolders.labelTokens().size();
    }

    @Override
    public int propertyKeyCount()
    {
        return tokenHolders.propertyKeyTokens().size();
    }

    @Override
    public int relationshipTypeCount()
    {
        return tokenHolders.relationshipTypeTokens().size();
    }

    @Override
    public boolean nodeExists( long id )
    {
        try
        {
            return stores.mainStore.exists( id );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public boolean relationshipExists( long id )
    {
        return false;
    }

    @Override
    public <T> T getOrCreateSchemaDependantState( Class<T> type, Function<StorageReader,T> factory )
    {
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    @Override
    public AllNodeScan allNodeScan()
    {
        return null;
    }

    @Override
    public AllRelationshipsScan allRelationshipScan()
    {
        return null;
    }

    @Override
    public StorageNodeCursor allocateNodeCursor()
    {
        return new FrekiNodeCursor( stores.mainStore );
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor()
    {
        return new FrekiPropertyCursor( stores.mainStore );
    }

    @Override
    public StorageRelationshipGroupCursor allocateRelationshipGroupCursor()
    {
        return new FrekiRelationshipGroupCursor( stores.mainStore );
    }

    @Override
    public StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor()
    {
        return new FrekiRelationshipTraversalCursor( stores.mainStore );
    }

    @Override
    public StorageRelationshipScanCursor allocateRelationshipScanCursor()
    {
        return new FrekiRelationshipScanCursor( stores.mainStore );
    }

    @Override
    public StorageSchemaReader schemaSnapshot()
    {
        return null;
    }

    @Override
    public TokenNameLookup tokenNameLookup()
    {
        return null;
    }
}
