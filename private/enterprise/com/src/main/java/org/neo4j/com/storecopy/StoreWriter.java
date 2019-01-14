/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

public interface StoreWriter extends Closeable
{
    /**
     * Pipe the data from the given {@link ReadableByteChannel} to a location given by the {@code path}, using the
     * given {@code temporaryBuffer} for buffering if necessary.
     * The {@code hasData} is an effect of the block format not supporting a zero length blocks, whereas a neostore
     * file may actually be 0 bytes we'll have to keep track of that special case.
     * The {@code requiredElementAlignment} parameter specifies the size in bytes to which the transferred elements
     * should be aligned. For record store files, this is the record size. For files that have no special alignment
     * requirements, you should use the value {@code 1} to signify that any alignment will do.
     */
    long write( String path, ReadableByteChannel data, ByteBuffer temporaryBuffer, boolean hasData,
                int requiredElementAlignment ) throws IOException;

    @Override
    void close();
}
