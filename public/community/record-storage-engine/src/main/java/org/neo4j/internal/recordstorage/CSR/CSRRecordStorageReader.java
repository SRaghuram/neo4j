package org.neo4j.internal.recordstorage.CSR;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.recordstorage.*;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.format.CSR.CSRBaseUtils;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.token.TokenHolders;

import java.util.ArrayList;

import static org.neo4j.token.api.TokenConstants.ANY_LABEL;

public class CSRRecordStorageReader extends RecordStorageReader {
    /**
     * All the nulls in this method is a testament to the fact that we probably need to break apart this reader,
     * separating index stuff out from store stuff.
     *
     * @param stores
     */
    public CSRRecordStorageReader(NeoStores stores) {
        super(stores);
    }
    private ArrayList<NeoStores> neoStores = new ArrayList<>();
    TokenHolders tokenHolders;
    RelationshipGroupDegreesStore groupDegreesStore;
    CountsAccessor counts;
    public CSRRecordStorageReader(TokenHolders tokenHolders, ArrayList<NeoStores> neoStores, CountsAccessor counts, RelationshipGroupDegreesStore groupDegreesStore,
                                  SchemaCache schemaCache) {

        super( tokenHolders,  neoStores.get( 0 ),  counts,  groupDegreesStore, schemaCache);
        this.neoStores = neoStores;
        this.tokenHolders = tokenHolders;
        this.groupDegreesStore = groupDegreesStore;
        this.counts = counts;
    }
    @Override
    public RecordNodeCursor allocateNodeCursor(PageCursorTracer cursorTracer ) {
        for (NeoStores neoStore : neoStores)
            neoStore.getRecordFormats().setParent( neoStore );
        return new CSRRecordNodeCursor(neoStores, groupDegreesStore, tokenHolders, cursorTracer);
        //return new CSRRecordNodeCursor(neoStores.getNodeStore(), neoStores.getRelationshipStore(), neoStores.getRelationshipGroupStore(),
        //        groupDegreesStore, tokenHolders, cursorTracer);
    }
    @Override
    public RecordRelationshipTraversalCursor allocateRelationshipTraversalCursor(PageCursorTracer cursorTracer )
    {
        for (NeoStores neoStore : neoStores)
            neoStore.getRecordFormats().setParent( neoStore );
        return new CSRRecordRelationshipTraversalCursor(neoStores, groupDegreesStore, cursorTracer);
    }

    @Override
    public RecordRelationshipScanCursor allocateRelationshipScanCursor(PageCursorTracer cursorTracer )
    {
        for (NeoStores neoStore : neoStores)
            neoStore.getRecordFormats().setParent( neoStore );
        return new CSRRecordRelationshipScanCursor(neoStores, cursorTracer);
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor(PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        return new CSRRecordPropertyCursor( neoStores, cursorTracer, memoryTracker );
    }
    @Override
    public long countsForNode( int labelId, PageCursorTracer cursorTracer )
    {
        return CSRBaseUtils.getInstance( neoStores.get( 0 ) ).getNodeCount( labelId );
        //return counts.nodeCount( labelId, cursorTracer );
    }

    @Override
    public long countsForRelationship( int startLabelId, int typeId, int endLabelId, PageCursorTracer cursorTracer )
    {
        if ( !(startLabelId == ANY_LABEL || endLabelId == ANY_LABEL) )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
        return counts.relationshipCount( startLabelId, typeId, endLabelId, cursorTracer );
    }
}
