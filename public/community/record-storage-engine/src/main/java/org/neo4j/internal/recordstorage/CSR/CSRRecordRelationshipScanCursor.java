package org.neo4j.internal.recordstorage.CSR;

import org.neo4j.internal.recordstorage.RecordRelationshipScanCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;

import java.util.ArrayList;

public class CSRRecordRelationshipScanCursor extends RecordRelationshipScanCursor {
    ArrayList<NeoStores> neoStores;
    public CSRRecordRelationshipScanCursor(ArrayList<NeoStores> neoStores, PageCursorTracer cursorTracer) {
        super( neoStores.get( 0 ).getRelationshipStore(), cursorTracer);
        this.neoStores = neoStores;
    }
}
