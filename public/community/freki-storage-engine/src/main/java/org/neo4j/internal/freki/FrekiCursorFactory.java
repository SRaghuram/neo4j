package org.neo4j.internal.freki;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.AllRelationshipsScan;

class FrekiCursorFactory
{
    private final MainStores stores;
    private final CursorAccessPatternTracer cursorAccessPatternTracer;

    FrekiCursorFactory( MainStores stores, CursorAccessPatternTracer cursorAccessPatternTracer )
    {
        this.stores = stores;
        this.cursorAccessPatternTracer = cursorAccessPatternTracer;
    }

    public AllRelationshipsScan allRelationshipScan()
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    public FrekiNodeCursor allocateNodeCursor( PageCursorTracer cursorTracer )
    {
        return new FrekiNodeCursor( stores, cursorAccessPatternTracer, cursorTracer );
    }

    public FrekiPropertyCursor allocatePropertyCursor( PageCursorTracer cursorTracer )
    {
        return new FrekiPropertyCursor( stores, cursorAccessPatternTracer, cursorTracer );
    }

    public FrekiRelationshipTraversalCursor allocateRelationshipTraversalCursor( PageCursorTracer cursorTracer )
    {
        return new FrekiRelationshipTraversalCursor( stores, cursorAccessPatternTracer, cursorTracer );
    }

    public FrekiRelationshipScanCursor allocateRelationshipScanCursor( PageCursorTracer cursorTracer )
    {
        return new FrekiRelationshipScanCursor( stores, cursorAccessPatternTracer, cursorTracer );
    }
}
