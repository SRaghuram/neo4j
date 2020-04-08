/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;

import com.neo4j.causalclustering.core.state.storage.StateMarshal;
import com.neo4j.causalclustering.messaging.EndOfStreamException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.memory.BufferScope;

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

    public StateRecoveryManager( FileSystemAbstraction fileSystem, StateMarshal<STATE> marshal )
    {
        this.fileSystem = fileSystem;
        this.marshal = marshal;
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

        STATE a = readLastEntryFrom( fileA );
        STATE b = readLastEntryFrom( fileB );

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

    private STATE readLastEntryFrom( File file ) throws IOException
    {
        try ( BufferScope bufferScope = new BufferScope( ReadAheadChannel.DEFAULT_READ_AHEAD_SIZE );
              ReadableChannel channel = new ReadAheadChannel<>( fileSystem.read( file ), bufferScope.buffer ) )
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
