/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.core.state.CoreStateFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class, LifeExtension.class} )
class DurableStateStorageTest
{
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private LifeSupport life;

    @Test
    void shouldMaintainStateGivenAnEmptyInitialStore() throws Exception
    {
        // given
        DurableStateStorage<AtomicInteger> storage = life.add( new DurableStateStorage<>( fileSystem, testDirectory.directory(),
                CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ), 100, NullLogProvider.getInstance() ) );

        // when
        storage.persistStoreData( new AtomicInteger( 99 ) );

        // then
        assertEquals( 4, fileSystem.getFileSize( stateFileA() ) );
    }

    @Test
    void shouldRotateToOtherStoreFileAfterSufficientEntries() throws Exception
    {
        // given
        final int numberOfEntriesBeforeRotation = 100;
        DurableStateStorage<AtomicInteger> storage = life.add( new DurableStateStorage<>( fileSystem, testDirectory.directory(),
                CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ), numberOfEntriesBeforeRotation, NullLogProvider.getInstance() ) );

        // when
        for ( int i = 0; i < numberOfEntriesBeforeRotation; i++ )
        {
            storage.persistStoreData( new AtomicInteger( i ) );
        }

        // Force the rotation
        storage.persistStoreData( new AtomicInteger( 9999 ) );

        // then
        assertEquals( 4, fileSystem.getFileSize( stateFileB() ) );
        assertEquals( numberOfEntriesBeforeRotation * 4, fileSystem.getFileSize( stateFileA() ) );
    }

    @Test
    void shouldRotateBackToFirstStoreFileAfterSufficientEntries() throws Exception
    {
        // given
        final int numberOfEntriesBeforeRotation = 100;
        DurableStateStorage<AtomicInteger> storage = life.add( new DurableStateStorage<>( fileSystem, testDirectory.directory(),
                CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ), numberOfEntriesBeforeRotation, NullLogProvider.getInstance() ) );

        // when
        for ( int i = 0; i < numberOfEntriesBeforeRotation * 2; i++ )
        {
            storage.persistStoreData( new AtomicInteger( i ) );
        }

        // Force the rotation back to the first store
        storage.persistStoreData( new AtomicInteger( 9999 ) );

        // then
        assertEquals( 4, fileSystem.getFileSize( stateFileA() ) );
        assertEquals( numberOfEntriesBeforeRotation * 4, fileSystem.getFileSize( stateFileB() ) );
    }

    @Test
    void shouldClearFileOnFirstUse() throws Throwable
    {
        // given
        int rotationCount = 10;

        DurableStateStorage<AtomicInteger> storage = new DurableStateStorage<>( fileSystem, testDirectory.directory(),
                CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ), rotationCount, NullLogProvider.getInstance() );
        int largestValueWritten = 0;
        try ( Lifespan lifespan = new Lifespan( storage ) )
        {
            for ( ; largestValueWritten < rotationCount * 2; largestValueWritten++ )
            {
                storage.persistStoreData( new AtomicInteger( largestValueWritten ) );
            }
        }

        // now both files are full. We reopen, then write some more.
        storage = life.add( new DurableStateStorage<>( fileSystem, testDirectory.directory(),
                CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ), rotationCount, NullLogProvider.getInstance() ) );

        storage.persistStoreData( new AtomicInteger( largestValueWritten++ ) );
        storage.persistStoreData( new AtomicInteger( largestValueWritten++ ) );
        storage.persistStoreData( new AtomicInteger( largestValueWritten ) );

        /*
         * We have written stuff in fileA but not gotten to the end (resulting in rotation). The largestValueWritten
         * should nevertheless be correct
         */
        ByteBuffer forReadingBackIn = ByteBuffer.allocate( 10_000 );
        StoreChannel lastWrittenTo = fileSystem.open( stateFileA(), OpenMode.READ );
        lastWrittenTo.read( forReadingBackIn );
        forReadingBackIn.flip();

        AtomicInteger lastRead = null;
        while ( true )
        {
            try
            {
                lastRead = new AtomicInteger( forReadingBackIn.getInt() );
            }
            catch ( BufferUnderflowException e )
            {
                break;
            }
        }

        // then
        assertNotNull( lastRead );
        assertEquals( largestValueWritten, lastRead.get() );
    }

    private static class AtomicIntegerMarshal extends SafeStateMarshal<AtomicInteger>
    {
        @Override
        public void marshal( AtomicInteger state, WritableChannel channel ) throws IOException
        {
            channel.putInt( state.intValue() );
        }

        @Override
        public AtomicInteger unmarshal0( ReadableChannel channel ) throws IOException
        {
            return new AtomicInteger( channel.getInt() );
        }

        @Override
        public AtomicInteger startState()
        {
            return new AtomicInteger( 0 );
        }

        @Override
        public long ordinal( AtomicInteger atomicInteger )
        {
            return atomicInteger.get();
        }
    }

    private File stateFileA()
    {
        return new File( new File( testDirectory.directory(), CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ).directoryName() ),
                CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ).baseName() + ".a" );
    }

    private File stateFileB()
    {
        return new File( new File( testDirectory.directory(), CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ).directoryName() ),
                CoreStateFiles.DUMMY( new AtomicIntegerMarshal() ).baseName() + ".b" );
    }
}
