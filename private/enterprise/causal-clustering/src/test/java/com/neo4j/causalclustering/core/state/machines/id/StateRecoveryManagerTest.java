/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.core.state.StateRecoveryManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.SafeStateMarshal;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralTestDirectoryExtension
class StateRecoveryManagerTest
{

    @Inject
    private TestDirectory testDir;
    @Inject
    private FileSystemAbstraction fsa;

    private final int NUMBER_OF_RECORDS_PER_FILE = 100;
    private final int NUMBER_OF_BYTES_PER_RECORD = 10;

    @BeforeEach
    void checkArgs()
    {
        Assertions.assertEquals( 0, NUMBER_OF_RECORDS_PER_FILE % NUMBER_OF_BYTES_PER_RECORD );
    }

    @Test
    void shouldFailIfBothFilesAreEmpty() throws Exception
    {
        // given
        fsa.mkdir( testDir.homePath() );

        Path fileA = fileA();
        fsa.write( fileA );

        Path fileB = fileB();
        fsa.write( fileB );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal(), INSTANCE );

        try
        {
            // when
            StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );
            Assertions.fail();
        }
        catch ( IllegalStateException ex )
        {
            // then
            // expected
        }
    }

    @Test
    void shouldReturnPreviouslyInactiveWhenOneFileFullAndOneEmpty() throws Exception
    {
        // given
        fsa.mkdir( testDir.homePath() );

        Path fileA = fileA();
        StoreChannel channel = fsa.write( fileA );

        fillUpAndForce( channel );

        Path fileB = fileB();
        fsa.write( fileB );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal(), INSTANCE );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        Assertions.assertEquals( fileB, recoveryStatus.activeFile() );
    }

    @Test
    void shouldReturnTheEmptyFileAsPreviouslyInactiveWhenActiveContainsCorruptEntry() throws Exception
    {
        // given
        fsa.mkdir( testDir.homePath() );

        Path fileA = fileA();
        StoreChannel channel = fsa.write( fileA );

        ByteBuffer buffer = writeLong( 999 );
        channel.writeAll( buffer );
        channel.force( false );

        Path fileB = fileB();
        channel = fsa.write( fileB );
        channel.close();

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal(), INSTANCE );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        Assertions.assertEquals( 999L, recoveryStatus.recoveredState() );
        Assertions.assertEquals( fileB, recoveryStatus.activeFile() );
    }

    @Test
    void shouldReturnTheFullFileAsPreviouslyInactiveWhenActiveContainsCorruptEntry()
            throws Exception
    {
        // given
        fsa.mkdir( testDir.homePath() );

        Path fileA = fileA();
        StoreChannel channel = fsa.write( fileA );

        ByteBuffer buffer = writeLong( 42 );
        channel.writeAll( buffer );
        channel.force( false );

        buffer.clear();
        buffer.putLong( 101 ); // extraneous bytes
        buffer.flip();
        channel.writeAll( buffer );
        channel.force( false );

        Path fileB = fileB();
        fsa.write( fileB );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal(), INSTANCE );

        // when
        final StateRecoveryManager.RecoveryStatus recoveryStatus = manager.recover( fileA, fileB );

        // then
        Assertions.assertEquals( fileB, recoveryStatus.activeFile() );
    }

    @Test
    void shouldRecoverFromPartiallyWrittenEntriesInBothFiles() throws Exception
    {
        // given
        fsa.mkdir( testDir.homePath() );

        StateRecoveryManager<Long> manager = new StateRecoveryManager<>( fsa, new LongMarshal(), INSTANCE );

        writeSomeLongsIn( fsa, fileA(), 3, 4 );
        writeSomeLongsIn( fsa, fileB(), 5, 6 );
        writeSomeGarbage( fsa, fileA() );
        writeSomeGarbage( fsa, fileB() );

        // when
        final StateRecoveryManager.RecoveryStatus recovered = manager.recover( fileA(), fileB() );

        // then
        Assertions.assertEquals( fileA(), recovered.activeFile() );
        Assertions.assertEquals( 6L, recovered.recoveredState() );
    }

    private Path fileA()
    {
        return testDir.homePath().resolve( "file.A" );
    }

    private Path fileB()
    {
        return testDir.homePath().resolve( "file.B" );
    }

    private void writeSomeGarbage( FileSystemAbstraction fsa, Path file ) throws IOException
    {
        final StoreChannel channel = fsa.write( file );
        ByteBuffer buffer = ByteBuffers.allocate( 4, INSTANCE );
        buffer.putInt( 9876 );
        buffer.flip();
        channel.writeAll( buffer );
        channel.force( false );
        channel.close();
    }

    private void writeSomeLongsIn( FileSystemAbstraction fsa, Path file, long... longs ) throws IOException
    {
        final StoreChannel channel = fsa.write( file );
        ByteBuffer buffer = ByteBuffers.allocate( longs.length * 8, INSTANCE );

        for ( long aLong : longs )
        {
            buffer.putLong( aLong );
        }

        buffer.flip();
        channel.writeAll( buffer );
        channel.force( false );
        channel.close();
    }

    private void fillUpAndForce( StoreChannel channel ) throws IOException
    {
        for ( int i = 0; i < NUMBER_OF_RECORDS_PER_FILE; i++ )
        {
            ByteBuffer buffer = writeLong( i );
            channel.writeAll( buffer );
            channel.force( false );
        }
    }

    private ByteBuffer writeLong( long logIndex )
    {
        ByteBuffer buffer = ByteBuffers.allocate( 8, INSTANCE );
        buffer.putLong( logIndex );
        buffer.flip();
        return buffer;
    }

    private static class LongMarshal extends SafeStateMarshal<Long>
    {
        @Override
        public Long startState()
        {
            return 0L;
        }

        @Override
        public long ordinal( Long aLong )
        {
            return aLong;
        }

        @Override
        public void marshal( Long aLong, WritableChannel channel ) throws IOException
        {
            channel.putLong( aLong );
        }

        @Override
        protected Long unmarshal0( ReadableChannel channel ) throws IOException
        {
            return channel.getLong();
        }
    }
}
