/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.LongIndexMarshal;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.neo4j.adversaries.CountingAdversary;
import org.neo4j.adversaries.MethodGuardedAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.SelectiveFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@EphemeralTestDirectoryExtension
class DurableStateStorageIT
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private Path dir;

    @BeforeEach
    void setUp()
    {
        dir = testDirectory.homePath();
    }

    @Test
    void shouldRecoverAfterCrashUnderLoad() throws Exception
    {
        MethodGuardedAdversary adversary = new MethodGuardedAdversary( new CountingAdversary( 100, true ),
                StoreChannel.class.getMethod( "writeAll", ByteBuffer.class ) );
        AdversarialFileSystemAbstraction adversarialFs = new AdversarialFileSystemAbstraction( adversary, fs );

        long lastValue = 0;
        try ( LongState persistedState = new LongState( adversarialFs, dir, 14 ) )
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            verifyException( adversary, expected );
        }

        try ( LongState restoredState = new LongState( fs, dir, 4 ) )
        {
            assertEquals( lastValue, restoredState.getTheState() );
        }
    }

    @Test
    void shouldProperlyRecoveryAfterCrashOnFileCreationDuringRotation() throws Exception
    {
        /*
         * Magic number warning. For a rotation threshold of 14, 998 operations on file A falls on truncation of the
         * file during rotation. This has been discovered via experimentation. The end result is that there is a
         * failure to create the file to rotate to. This should be recoverable.
         */
        MethodGuardedAdversary adversary = new MethodGuardedAdversary( new CountingAdversary( 20, true ),
                FileSystemAbstraction.class.getMethod( "truncate", File.class, long.class ) );
        AdversarialFileSystemAbstraction breakingFSA = new AdversarialFileSystemAbstraction( adversary, fs );
        SelectiveFileSystemAbstraction combinedFSA = new SelectiveFileSystemAbstraction( dir.resolve( "dummy.a" ).toFile(), breakingFSA, fs );

        long lastValue = 0;
        try ( LongState persistedState = new LongState( combinedFSA, dir, 14 ) )
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            verifyException( adversary, expected );
        }

        try ( LongState restoredState = new LongState( fs, dir, 14 ) )
        {
            assertEquals( lastValue, restoredState.getTheState() );
        }
    }

    @Test
    void shouldProperlyRecoveryAfterCrashOnFileForceDuringWrite() throws Exception
    {
        /*
         * Magic number warning. For a rotation threshold of 14, 990 operations on file A falls on a force() of the
         * current active file. This has been discovered via experimentation. The end result is that there is a
         * flush (but not write) a value. This should be recoverable. Interestingly, the failure semantics are a bit
         * unclear on what should happen to that value. We assume that exception during persistence requires recovery
         * to discover if the last argument made it to disk or not. Since we use an EFSA, force is not necessary and
         * the value that caused the failure is actually "persisted" and recovered.
         */
        MethodGuardedAdversary adversary = new MethodGuardedAdversary( new CountingAdversary( 40, true ),
                StoreChannel.class.getMethod( "force", boolean.class ) );
        AdversarialFileSystemAbstraction breakingFSA = new AdversarialFileSystemAbstraction( adversary, fs );
        SelectiveFileSystemAbstraction combinedFSA = new SelectiveFileSystemAbstraction( dir.resolve( "dummy.a" ).toFile(), breakingFSA, fs );

        long lastValue = 0;

        try ( LongState persistedState = new LongState( combinedFSA, dir, 14 ) )
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            verifyException( adversary, expected );
        }

        try ( LongState restoredState = new LongState( fs, dir, 14 ) )
        {
            assertThat( restoredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
        }
    }

    @Test
    void shouldProperlyRecoveryAfterCrashingDuringRecovery() throws Exception
    {
        long lastValue = 0;

        try ( LongState persistedState = new LongState( fs, dir, 14 ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }

        // We create a new state that will attempt recovery. The AFS will make it fail on open() of one of the files
        MethodGuardedAdversary adversary = new MethodGuardedAdversary( new CountingAdversary( 1, true ),
                FileSystemAbstraction.class.getMethod( "read", File.class ) );
        AdversarialFileSystemAbstraction adversarialFs = new AdversarialFileSystemAbstraction( adversary, fs );

        Exception error = assertThrows( Exception.class, () -> new LongState( adversarialFs, dir, 14 ) );

        // stack trace should contain read()
        verifyException( adversary, error.getCause() );

        // Recovery over the normal filesystem after a failed recovery should proceed correctly
        try ( LongState recoveredState = new LongState( fs, dir, 14 ) )
        {
            assertThat( recoveredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
        }
    }

    @Test
    void shouldProperlyRecoveryAfterCloseOnActiveFileDuringRotation() throws Exception
    {
        MethodGuardedAdversary adversary = new MethodGuardedAdversary( new CountingAdversary( 5, true ),
                StoreChannel.class.getMethod( "close" ) );
        AdversarialFileSystemAbstraction breakingFSA = new AdversarialFileSystemAbstraction( adversary, fs );
        SelectiveFileSystemAbstraction combinedFSA = new SelectiveFileSystemAbstraction( dir.resolve( "dummy.a" ).toFile(), breakingFSA, fs );

        long lastValue = 0;
        try ( LongState persistedState = new LongState( combinedFSA, dir, 14 ) )
        {
            while ( true ) // it will break from the Exception that AFS will throw
            {
                long tempValue = lastValue + 1;
                persistedState.setTheState( tempValue );
                lastValue = tempValue;
            }
        }
        catch ( Exception expected )
        {
            // this stack trace should contain close()
            verifyException( adversary, expected );
        }

        try ( LongState restoredState = new LongState( fs, dir, 14 ) )
        {
            assertThat( restoredState.getTheState(), greaterThanOrEqualTo( lastValue ) );
        }
    }

    private static void verifyException( MethodGuardedAdversary adversary, Throwable expectedException )
    {
        assertTrue( adversary.getLastAdversaryException().isPresent() );
        assertSame( adversary.getLastAdversaryException().get(), expectedException );
    }

    private static class LongState implements AutoCloseable
    {
        private final DurableStateStorage<Long> stateStorage;
        private long theState;
        private final LifeSupport lifeSupport = new LifeSupport();

        LongState( FileSystemAbstraction fileSystemAbstraction, Path stateDir,
                int numberOfEntriesBeforeRotation )
        {
            lifeSupport.start();

            this.stateStorage = lifeSupport.add( new DurableStateStorage<>( fileSystemAbstraction, stateDir, CoreStateFiles.DUMMY( new LongIndexMarshal() ),
                    numberOfEntriesBeforeRotation, NullLogProvider.getInstance(), INSTANCE
            ) );

            this.theState = this.stateStorage.getInitialState();
        }

        long getTheState()
        {
            return theState;
        }

        void setTheState( long newState ) throws IOException
        {
            stateStorage.writeState( newState );
            this.theState = newState;
        }

        @Override
        public void close()
        {
            lifeSupport.shutdown();
        }
    }
}
