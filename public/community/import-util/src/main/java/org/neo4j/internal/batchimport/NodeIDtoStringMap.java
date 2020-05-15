package org.neo4j.internal.batchimport;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.util.function.LongFunction;

public abstract class NodeIDtoStringMap implements LongFunction<Object> {
    public abstract void save(long nodeId, Object object, PageCursorTracer cursorTracer);
    public abstract Object apply(long nodeId);
    public abstract void close();
}
