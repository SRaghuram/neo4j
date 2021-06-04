package org.neo4j.internal.recordstorage.CSR;

import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.recordstorage.RecordNodeCursor;
import org.neo4j.internal.recordstorage.RecordRelationshipTraversalCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.*;
import org.neo4j.kernel.impl.store.format.CSR.CSRBaseUtils;
import org.neo4j.storageengine.api.*;
import org.neo4j.token.TokenHolders;

import java.util.ArrayList;

public class CSRRecordNodeCursor extends RecordNodeCursor {
    ArrayList<NeoStores> neoStores;

    public CSRRecordNodeCursor(ArrayList<NeoStores> neoStores,
                               RelationshipGroupDegreesStore groupDegreesStore, TokenHolders tokenHolders, PageCursorTracer cursorTracer) {
        super(neoStores.get(0).getNodeStore(), neoStores.get(0).getRelationshipStore(), neoStores.get(0).getRelationshipGroupStore(), groupDegreesStore, cursorTracer);
        this.neoStores = neoStores;
        CSRBaseUtils.getInstance(neoStores.get(0), tokenHolders);
    }

    @Override
    public void properties(StoragePropertyCursor propertyCursor) {
        propertyCursor.initNodeProperties(getNextProp(), getId());
    }

    @Override
    public void scan() {
        this.neoStores.get(0).getNodeStore().getPagedFile().increaseLastPageIdTo(0);
        super.scan();
    }

    @Override
    public void single(long reference) {
        super.single(reference);
    }

    @Override
    public boolean scanBatch(AllNodeScan scan, int sizeHint) {
        return super.scanBatch(scan, sizeHint);
    }

    @Override
    public boolean hasProperties() {
        return super.hasProperties();
    }

    @Override
    public void relationships(StorageRelationshipTraversalCursor traversalCursor, RelationshipSelection selection) {
        super.relationships(traversalCursor, selection);
    }

    @Override
    public int[] relationshipTypes() {
        return super.relationshipTypes();
    }

    @Override
    public void degrees(RelationshipSelection selection, Degrees.Mutator mutator, boolean allowFastDegreeLookup) {
        super.degrees( selection, mutator, allowFastDegreeLookup );
    }

}
