/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines;

import com.neo4j.causalclustering.core.state.machines.id.CommandIndexTracker;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;

import static com.neo4j.dbms.TransactionEventService.TransactionCommitNotifier;
import static org.mockito.Mockito.mock;

public class DummyStateMachineCommitHelper extends StateMachineCommitHelper
{
    public DummyStateMachineCommitHelper()
    {
        this( new CommandIndexTracker(), PageCursorTracer.NULL );
    }

    public DummyStateMachineCommitHelper( CommandIndexTracker commandIndexTracker, PageCursorTracer pageCursorTracer )
    {
        super( commandIndexTracker, () -> pageCursorTracer, EmptyVersionContextSupplier.EMPTY, mock( TransactionCommitNotifier.class ) );
    }
}
