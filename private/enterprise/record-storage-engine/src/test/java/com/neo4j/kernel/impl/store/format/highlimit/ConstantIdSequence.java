/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.internal.id.IdRange;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

class ConstantIdSequence implements IdSequence
{
    @Override
    public long nextId( PageCursorTracer ignored )
    {
        return 1;
    }

    @Override
    public IdRange nextIdBatch( int size, PageCursorTracer ignored )
    {
        throw new UnsupportedOperationException();
    }
}
