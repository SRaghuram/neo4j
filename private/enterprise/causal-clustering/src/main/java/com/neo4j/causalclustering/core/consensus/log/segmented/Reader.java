/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

import static java.nio.file.StandardOpenOption.READ;

public class Reader implements Closeable
{
    private final long version;
    private final StoreChannel storeChannel;
    private long timeStamp;

    Reader( FileSystemAbstraction fsa, File file, long version ) throws IOException
    {
        this.storeChannel = fsa.open( file, Set.of( READ ) );
        this.version = version;
    }

    public long version()
    {
        return version;
    }

    public StoreChannel channel()
    {
        return storeChannel;
    }

    @Override
    public void close() throws IOException
    {
        storeChannel.close();
    }

    void setTimeStamp( long timeStamp )
    {
        this.timeStamp = timeStamp;
    }

    long getTimeStamp()
    {
        return timeStamp;
    }
}
