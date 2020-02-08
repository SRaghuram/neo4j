package org.neo4j.storageengine;

import org.neo4j.collection.PrimitiveLongCollections;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.counts.CountsStore;
import org.neo4j.internal.recordstorage.SchemaCache;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.MyStore;
import org.neo4j.kernel.impl.store.cursors.MyNodeCursor;
import org.neo4j.kernel.impl.store.cursors.MyPropertyCursor;
import org.neo4j.kernel.impl.store.cursors.MyRelationshipTraversalCursor;
import org.neo4j.kernel.impl.store.scan.MyAllNodeScan;
import org.neo4j.kernel.impl.store.scan.MyAllRelationshipScan;
import org.neo4j.kernel.impl.store.scan.MyRelationshipScanCursor;
import org.neo4j.storageengine.api.*;
import org.neo4j.token.TokenHolders;

import java.util.Collection;
import java.util.Iterator;
import java.util.OptionalLong;
import java.util.function.Function;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

public class MyStorageReader implements StorageReader {

    MyStore store;
    TokenHolders tokenHolders;
    CountsStore countsStore;
    SchemaCache schemaCache;
    MyStorageReader(TokenHolders tokenHolders, MyStore store, CountsStore countsStore, SchemaCache schemaCache)
    {
        this.store = store;
        this.tokenHolders = tokenHolders;
        this.countsStore = countsStore;
        this.schemaCache = schemaCache;
    }
    @Override
    public void close() {

    }

    @Override
    public boolean indexExists(IndexDescriptor index) {
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
    public Long indexGetOwningUniquenessConstraintId(IndexDescriptor index) {
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
    public long countsForNode(int labelId,  PageCursorTracer cursorTracer) {
        return store.getNumNodes( labelId );
    }

    @Override
    public long countsForRelationship(int startLabelId, int typeId, int endLabelId, PageCursorTracer cursorTracer) {

        return store.getNumRels(startLabelId, typeId, endLabelId);
    }

    @Override
    public long nodesGetCount( PageCursorTracer cursorTracer) {
        return store.getNumNodes();
    }

    @Override
    public long relationshipsGetCount() {
        return store.getNumRels();
    }

    @Override
    public int labelCount() {
        return store.getNum(MyStore.KEYTYPE.LABEL);
    }

    @Override
    public int propertyKeyCount() {
        return store.getNum(MyStore.KEYTYPE.PROPERTY);
    }

    @Override
    public int relationshipTypeCount() {
        return store.getNum(MyStore.KEYTYPE.RELTYPE);
    }

    @Override
    public boolean nodeExists(long id, PageCursorTracer cursorTracer ) {
        return store.isInUse(id, MyStore.MyStoreType.NODE);
    }

    @Override
    public boolean relationshipExists(long id, PageCursorTracer cursorTracer ) {
        return store.isInUse( id, MyStore.MyStoreType.RELATIONSHIP);
    }

    @Override
    public <T> T getOrCreateSchemaDependantState(Class<T> type, Function<StorageReader, T> factory) {
        String loader = this.getClass().getClassLoader().getName();
        return schemaCache.getOrCreateDependantState( type, factory, this );
    }

    @Override
    public AllNodeScan allNodeScan() {
        return new MyAllNodeScan(store);
    }

    @Override
    public AllRelationshipsScan allRelationshipScan() {
        return new MyAllRelationshipScan(store);
    }

    @Override
    public StorageNodeCursor allocateNodeCursor( PageCursorTracer cursorTracer ) {
        return new MyNodeCursor(store);
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer ) {

        return new MyPropertyCursor(store);
    }



    @Override
    public StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer ) {
        return new MyRelationshipTraversalCursor(store);
    }

    @Override
    public StorageRelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer ) {
        return new MyRelationshipScanCursor(store);
    }

    @Override
    public StorageSchemaReader schemaSnapshot() {
        return null;
    }

    @Override
    public IndexDescriptor indexGetForName(String name) {
        return null;
    }

    @Override
    public ConstraintDescriptor constraintGetForName(String name) {
        return null;
    }

    @Override
    public Iterator<IndexDescriptor> indexGetForSchema(SchemaDescriptor descriptor) {
        return new Iterator<IndexDescriptor>(){

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public IndexDescriptor next() {
                return null;
            }
        };
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel(int labelId) {

        return new Iterator<IndexDescriptor>(){

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public IndexDescriptor next() {
                return null;
            }
        };
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForRelationshipType(int relationshipType) {
        return new Iterator<IndexDescriptor>(){

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public IndexDescriptor next() {
                return null;
            }
        };
    }

    @Override
    public TokenNameLookup tokenNameLookup()
    {
        return null;
    }
}
