package org.neo4j.internal.batchimport;

import org.eclipse.collections.api.iterator.LongIterator;

public interface IdIterator {
    /**
     * @return next batch of ids as {@link LongIterator}, or {@code null} if there are no more ids to return.
     */
    LongIterator nextBatch();
}

