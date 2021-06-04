package org.neo4j.internal.recordstorage.CSR;

import org.neo4j.internal.counts.RelationshipGroupDegreesStore;
import org.neo4j.internal.recordstorage.RecordRelationshipTraversalCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.StoreVersion;
import org.neo4j.storageengine.api.RelationshipSelection;

import java.util.ArrayList;

public class CSRRecordRelationshipTraversalCursor extends RecordRelationshipTraversalCursor {
    ArrayList<NeoStores> neoStores;
    public CSRRecordRelationshipTraversalCursor(ArrayList<NeoStores> neoStores,
                                                //RelationshipStore relationshipStore, RelationshipGroupStore groupStore,
                                                RelationshipGroupDegreesStore groupDegreesStore, PageCursorTracer cursorTracer) {
        super(neoStores.get(0).getRelationshipStore(), neoStores.get(0).getRelationshipGroupStore(), groupDegreesStore, cursorTracer);
        this.neoStores = neoStores;
        for (NeoStores neoStore: neoStores)
            if (neoStore.getRecordFormats().storeVersion().equals(StoreVersion.CSR_V1_6_0))
                neoStore.getRelationshipStore().getPagedFile().increaseLastPageIdTo( 0 );
    }

    @Override
    public void init( long nodeReference, long reference, RelationshipSelection selection )
    {
        super.init( nodeReference, reference, selection );
    }
    @Override
    public boolean next()
    {
        if (super.next())
            return true;
        //switch to next layer, and only after the last layer return false
        return false;
    }
}
