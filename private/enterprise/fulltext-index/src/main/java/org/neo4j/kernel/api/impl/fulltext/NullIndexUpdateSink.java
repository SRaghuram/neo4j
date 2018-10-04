/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.schema.IndexReader;

/**
 * An implementation of {@link IndexUpdateSink} that does not actually do anything.
 */
public class NullIndexUpdateSink extends IndexUpdateSink
{
    public static final NullIndexUpdateSink INSTANCE = new NullIndexUpdateSink();

    private NullIndexUpdateSink()
    {
        super( null, 0 );
    }

    @Override
    public void enqueueUpdate( DatabaseIndex<? extends IndexReader> index, IndexUpdater indexUpdater, IndexEntryUpdate<?> update )
    {
    }

    @Override
    public void closeUpdater( DatabaseIndex<? extends IndexReader> index, IndexUpdater indexUpdater )
    {
    }

    @Override
    public void awaitUpdateApplication()
    {
    }
}
