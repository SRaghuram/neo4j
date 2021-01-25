/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;

import static com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventDispatch;
import static org.mockito.Mockito.mock;

public class DummyStateMachineCommitHelper extends StateMachineCommitHelper
{
    public DummyStateMachineCommitHelper()
    {
        this( new CommandIndexTracker(), PageCacheTracer.NULL );
    }

    public DummyStateMachineCommitHelper( CommandIndexTracker commandIndexTracker, PageCacheTracer cacheTracer )
    {
        super( commandIndexTracker, EmptyVersionContextSupplier.EMPTY, mock( ReplicatedDatabaseEventDispatch.class ), cacheTracer );
    }
}
