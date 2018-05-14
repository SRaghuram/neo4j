/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.unsafe.impl.batchimport;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.StandardCopyOption;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;
import org.neo4j.storageengine.api.ReadPastEndException;

import static com.neo4j.unsafe.impl.batchimport.ChannelUtils.readString;
import static com.neo4j.unsafe.impl.batchimport.ChannelUtils.writeString;
import static org.neo4j.io.fs.OpenMode.READ;
import static org.neo4j.io.fs.OpenMode.READ_WRITE;
import static org.neo4j.kernel.impl.store.PropertyType.EMPTY_BYTE_ARRAY;

public class StateStorage
{
    public static final String NO_STATE = "";
    public static final String INIT = "init";

    private final FileSystemAbstraction fs;
    private final File stateFile;
    private final File tempFile;

    public StateStorage( FileSystemAbstraction fs, File stateFile )
    {
        this.fs = fs;
        this.stateFile = stateFile;
        this.tempFile = new File( stateFile.getAbsolutePath() + ".tmp" );
    }

    public Pair<String,byte[]> get() throws IOException
    {
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fs.open( stateFile, READ ) ) )
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
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.open( tempFile, READ_WRITE ) ) )
        {
            writeString( name, channel );
            channel.putInt( checkPoint.length );
            channel.put( checkPoint, checkPoint.length );
        }
        fs.renameFile( tempFile, stateFile, StandardCopyOption.ATOMIC_MOVE );
    }

    public void remove() throws IOException
    {
        fs.renameFile( stateFile, tempFile, StandardCopyOption.ATOMIC_MOVE );
        fs.deleteFileOrThrow( tempFile );
    }
}
