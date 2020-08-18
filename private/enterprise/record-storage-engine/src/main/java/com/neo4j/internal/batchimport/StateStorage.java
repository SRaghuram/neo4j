/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;

import static com.neo4j.internal.batchimport.ChannelUtils.readString;
import static com.neo4j.internal.batchimport.ChannelUtils.writeString;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;
import static org.neo4j.kernel.impl.store.PropertyType.EMPTY_BYTE_ARRAY;

public class StateStorage
{
    static final String NO_STATE = "";
    static final String INIT = "init";

    private final FileSystemAbstraction fs;
    private final Path stateFile;
    private final Path tempFile;
    private final MemoryTracker memoryTracker;

    StateStorage( FileSystemAbstraction fs, Path stateFile, MemoryTracker memoryTracker )
    {
        this.fs = fs;
        this.stateFile = stateFile;
        this.tempFile = stateFile.resolveSibling( stateFile.getFileName() + ".tmp" );
        this.memoryTracker = memoryTracker;
    }

    public Pair<String,byte[]> get() throws IOException
    {
        if ( Files.notExists( stateFile ) )
        {
            return Pair.of( NO_STATE, EMPTY_BYTE_ARRAY );
        }
        try ( ReadableChannel channel = new ReadAheadChannel<>( fs.read( stateFile.toFile() ),
                new NativeScopedBuffer( DEFAULT_READ_AHEAD_SIZE, memoryTracker ) ) )
        {
            String name = readString( channel );
            byte[] checkPoint = new byte[channel.getInt()];
            channel.get( checkPoint, checkPoint.length );
            return Pair.of( name, checkPoint );
        }
        catch ( FileNotFoundException e )
        {
            return Pair.of( NO_STATE, EMPTY_BYTE_ARRAY );
        }
        catch ( ReadPastEndException e )
        {
            // Unclear why this is happening. Maybe you are running windows or something?
            // Seems to be due to a completely empty file. Simply fall back to INIT state
            return Pair.of( INIT, EMPTY_BYTE_ARRAY );
        }
    }

    public void set( String name, byte[] checkPoint ) throws IOException
    {
        fs.mkdirs( tempFile.getParent().toFile() );
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.write( tempFile.toFile() ), memoryTracker ) )
        {
            writeString( name, channel );
            channel.putInt( checkPoint.length );
            channel.put( checkPoint, checkPoint.length );
        }
        fs.renameFile( tempFile.toFile(), stateFile.toFile(), ATOMIC_MOVE );
    }

    public void remove() throws IOException
    {
        fs.renameFile( stateFile.toFile(), tempFile.toFile(), ATOMIC_MOVE );
        fs.deleteFileOrThrow( tempFile.toFile() );
    }
}
