package org.neo4j.kernel.impl.store;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.token.api.NamedToken;

import java.io.IOException;
import java.util.List;

public interface TokenStoreInterface {

    public int nextTokenId( PageCursorTracer cursorTracer );
    public long[] getOrCreateIds( String[] stringValues );
    public int getOrCreateId( String stringValue );
    public long[] getOrCreateIds( String[] stringValues, int cursor );
    public void flush( PageCursorTracer cursorTracer );
    public int getHighId();
}
