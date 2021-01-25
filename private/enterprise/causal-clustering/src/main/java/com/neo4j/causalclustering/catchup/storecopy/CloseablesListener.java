/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.IOUtils;

class CloseablesListener implements AutoCloseable, GenericFutureListener<Future<Void>>
{
    private final List<AutoCloseable> closeables = new ArrayList<>();

    <T extends AutoCloseable> T add( T closeable )
    {
        if ( closeable == null )
        {
            throw new IllegalArgumentException( "closeable cannot be null!" );
        }
        closeables.add( closeable );
        return closeable;
    }

    @Override
    public void close()
    {
        IOUtils.close( RuntimeException::new, closeables );
    }

    @Override
    public void operationComplete( Future<Void> future )
    {
        close();
    }
}
