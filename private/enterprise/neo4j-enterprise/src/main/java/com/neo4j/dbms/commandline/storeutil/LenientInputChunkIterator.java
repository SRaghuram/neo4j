/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.commandline.storeutil;

import java.io.IOException;

import org.neo4j.internal.batchimport.InputIterator;
import org.neo4j.internal.batchimport.input.InputChunk;
import org.neo4j.kernel.impl.store.CommonAbstractStore;

import static java.lang.Long.min;

public abstract class LenientInputChunkIterator implements InputIterator
{
    private final int batchSize;
    private final long highId;
    private long id;

    LenientInputChunkIterator( CommonAbstractStore<?,?> store )
    {
        this.batchSize = store.getRecordsPerPage() * 10;
        try
        {
            this.highId = store.getRecordsPerPage() * (store.getLastPageId() + 1);
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public synchronized boolean next( InputChunk chunk )
    {
        if ( id >= highId )
        {
            return false;
        }
        long startId = id;
        id = min( highId, startId + batchSize );
        ((LenientStoreInputChunk) chunk).setChunkRange( startId, id );
        return true;
    }

    @Override
    public void close()
    {
    }
}
