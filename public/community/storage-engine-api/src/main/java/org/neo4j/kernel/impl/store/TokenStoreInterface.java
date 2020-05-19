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
    //public List<NamedToken> loadTokens(PageCursorTracer cursorTracer ) throws IOException;
    //public NamedToken loadToken( int id, PageCursorTracer cursorTracer ) throws IOException;
    //public void setHighId( int newHighId );
    //public void writeToken( NamedToken token, PageCursorTracer cursorTracer ) throws IOException;
}
