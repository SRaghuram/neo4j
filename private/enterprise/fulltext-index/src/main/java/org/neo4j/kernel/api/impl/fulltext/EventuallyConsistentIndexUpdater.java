/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.api.impl.fulltext;

import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.schema.IndexReader;

class EventuallyConsistentIndexUpdater implements IndexUpdater
{
    private final DatabaseIndex<? extends IndexReader> index;
    private final IndexUpdater indexUpdater;
    private final IndexUpdateSink indexUpdateSink;

    EventuallyConsistentIndexUpdater( DatabaseIndex<? extends IndexReader> index, IndexUpdater indexUpdater, IndexUpdateSink indexUpdateSink )
    {
        this.index = index;
        this.indexUpdater = indexUpdater;
        this.indexUpdateSink = indexUpdateSink;
    }

    @Override
    public void process( IndexEntryUpdate<?> update )
    {
        indexUpdateSink.enqueueUpdate( index, indexUpdater, update );
    }

    @Override
    public void close()
    {
        indexUpdateSink.closeUpdater( index, indexUpdater );
    }
}
