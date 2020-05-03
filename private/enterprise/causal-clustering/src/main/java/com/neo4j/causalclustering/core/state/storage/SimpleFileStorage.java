/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.ChannelMarshal;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.io.ByteUnit.kibiBytes;

public class SimpleFileStorage<T> implements SimpleStorage<T>
{
    private final FileSystemAbstraction fileSystem;
    private final ChannelMarshal<T> marshal;
    private final MemoryTracker memoryTracker;
    private final File file;
    private final Log log;

    public SimpleFileStorage( FileSystemAbstraction fileSystem, File file, ChannelMarshal<T> marshal, LogProvider logProvider, MemoryTracker memoryTracker )
    {
        this.fileSystem = fileSystem;
        this.log = logProvider.getLog( getClass() );
        this.file = file;
        this.marshal = marshal;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public boolean exists()
    {
        return fileSystem.fileExists( file );
    }

    @Override
    public T readState() throws IOException
    {
        try ( NativeScopedBuffer bufferScope = new NativeScopedBuffer( ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE, memoryTracker );
              ReadableChannel channel = new ReadAheadChannel<>( fileSystem.read( file ), bufferScope.getBuffer() ) )
        {
            return marshal.unmarshal( channel );
        }
        catch ( EndOfStreamException e )
        {
            log.error( "End of stream reached: " + file );
            throw new IOException( e );
        }
    }

    @Override
    public void writeState( T state ) throws IOException
    {
        if ( file.getParentFile() != null )
        {
            fileSystem.mkdirs( file.getParentFile() );
        }
        fileSystem.deleteFile( file );

        try ( FlushableChannel channel = new PhysicalFlushableChannel( fileSystem.write( file ), new NativeScopedBuffer( kibiBytes( 512 ), memoryTracker ) ) )
        {
            marshal.marshal( state, channel );
        }
    }

}
