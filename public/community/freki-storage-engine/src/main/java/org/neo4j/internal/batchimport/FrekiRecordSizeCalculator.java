package org.neo4j.internal.batchimport;

import org.neo4j.internal.batchimport.input.PropertySizeCalculator;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

import java.util.function.ToIntBiFunction;

public class FrekiRecordSizeCalculator implements PropertySizeCalculator {

    @Override
    public int calculateSize(Value[] values, PageCursorTracer cursorTracer, MemoryTracker memoryTracker) {
        return 0;
    }
}
