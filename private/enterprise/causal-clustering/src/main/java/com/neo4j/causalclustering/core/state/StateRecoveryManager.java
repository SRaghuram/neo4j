/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.marshal.EndOfStreamException;
import com.neo4j.causalclustering.core.state.storage.StateMarshal;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.io.fs.ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE;

public class StateRecoveryManager<STATE>
{
    public static class RecoveryStatus<STATE>
    {
        private final File activeFile;
        private final STATE recoveredState;

        RecoveryStatus( File activeFile, STATE recoveredState )
        {
            this.activeFile = activeFile;
            this.recoveredState = recoveredState;
        }

        public STATE recoveredState()
        {
            return recoveredState;
        }

        public File activeFile()
        {
            return activeFile;
        }
    }

    protected final FileSystemAbstraction fileSystem;
    private final StateMarshal<STATE> marshal;
    private final MemoryTracker memoryTracker;

    public StateRecoveryManager( FileSystemAbstraction fileSystem, StateMarshal<STATE> marshal, MemoryTracker memoryTracker )
    {
        this.fileSystem = fileSystem;
        this.marshal = marshal;
        this.memoryTracker = memoryTracker;
    }

    /**
     * @return RecoveryStatus containing the previously active and previously inactive files. The previously active
     * file contains the latest readable log index (though it may also contain some garbage) and the inactive file is
     * safe to become the new state holder.
     * @throws IOException if any IO goes wrong.
     */
    public RecoveryStatus<STATE> recover( File fileA, File fileB ) throws IOException
    {
        assert fileA != null && fileB != null;

        STATE a = readLastEntryFrom( fileA, memoryTracker );
        STATE b = readLastEntryFrom( fileB, memoryTracker );

        if ( a == null && b == null )
        {
            throw new IllegalStateException( "no recoverable state" );
        }

        if ( a == null )
        {
            return new RecoveryStatus<>( fileA, b );
        }
        else if ( b == null )
        {
            return new RecoveryStatus<>( fileB, a );
        }
        else if ( marshal.ordinal( a ) > marshal.ordinal( b ) )
        {
            return new RecoveryStatus<>( fileB, a );
        }
        else
        {
            return new RecoveryStatus<>( fileA, b );
        }
    }

    private STATE readLastEntryFrom( File file, MemoryTracker memoryTracker ) throws IOException
    {
        try ( ReadableChannel channel = new ReadAheadChannel<>( fileSystem.read( file ), new NativeScopedBuffer( DEFAULT_READ_AHEAD_SIZE, memoryTracker ) ) )
        {
            STATE result = null;
            STATE lastRead;

            try
            {
                while ( (lastRead = marshal.unmarshal( channel )) != null )
                {
                    result = lastRead;
                }
            }
            catch ( EndOfStreamException e )
            {
                // ignore; just use previous complete entry
            }

            return result;
        }
    }
}
