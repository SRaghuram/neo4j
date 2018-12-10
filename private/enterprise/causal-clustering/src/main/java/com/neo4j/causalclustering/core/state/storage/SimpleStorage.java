/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import java.io.IOException;

import org.neo4j.function.ThrowingConsumer;

public interface SimpleStorage<T> extends StateStorage<T>
{
    boolean exists();

    T readState() throws IOException;

    void writeState( T state ) throws IOException;

    default <E extends Exception> void writeOrVerify( T state, ThrowingConsumer<T, E> verify ) throws E, IOException
    {
        if ( exists() )
        {
            verify.accept( readState() );
        }
        else
        {
            writeState( state );
        }
    }

    @Override
    default T getInitialState()
    {
        return null;
    }

    @Override
    default void persistStoreData( T t ) throws IOException
    {
        writeState( t );
    }
}
