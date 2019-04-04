/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.internal.batchimport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableClosableChannel;

import static com.neo4j.internal.batchimport.ChannelUtils.readString;
import static com.neo4j.internal.batchimport.ChannelUtils.writeString;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static org.neo4j.kernel.impl.store.PropertyType.EMPTY_BYTE_ARRAY;

public class StateStorage
{
    static final String NO_STATE = "";
    static final String INIT = "init";

    private final FileSystemAbstraction fs;
    private final File stateFile;
    private final File tempFile;

    StateStorage( FileSystemAbstraction fs, File stateFile )
    {
        this.fs = fs;
        this.stateFile = stateFile;
        this.tempFile = new File( stateFile.getAbsolutePath() + ".tmp" );
    }

    public Pair<String,byte[]> get() throws IOException
    {
        if ( !stateFile.exists() )
        {
            return Pair.of( NO_STATE, EMPTY_BYTE_ARRAY );
        }
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fs.read( stateFile ) ) )
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
        fs.mkdirs( tempFile.getParentFile() );
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.write( tempFile ) ) )
        {
            writeString( name, channel );
            channel.putInt( checkPoint.length );
            channel.put( checkPoint, checkPoint.length );
        }
        fs.renameFile( tempFile, stateFile, ATOMIC_MOVE );
    }

    public void remove() throws IOException
    {
        fs.renameFile( stateFile, tempFile, ATOMIC_MOVE );
        fs.deleteFileOrThrow( tempFile );
    }
}
