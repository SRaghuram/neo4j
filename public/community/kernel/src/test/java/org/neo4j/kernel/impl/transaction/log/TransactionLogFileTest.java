/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.IncompleteLogHeaderException;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.util.concurrent.Futures;

import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.kernel.impl.transaction.log.TestLogEntryReader.logEntryReader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogVersions.CURRENT_FORMAT_LOG_HEADER_SIZE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

@Neo4jLayoutExtension
@ExtendWith( LifeExtension.class )
class TransactionLogFileTest
{
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private FileSystemAbstraction fileSystem;
    @Inject
    private LifeSupport life;

    private CapturingChannelFileSystem wrappingFileSystem;

    private final long rotationThreshold = ByteUnit.mebiBytes( 1 );
    private final LogVersionRepository logVersionRepository = new SimpleLogVersionRepository( 1L );
    private final TransactionIdStore transactionIdStore = new SimpleTransactionIdStore( 2L, 0, BASE_TX_COMMIT_TIMESTAMP, 0, 0 );

    @BeforeEach
    void setUp()
    {
        wrappingFileSystem = new CapturingChannelFileSystem( fileSystem );
    }

    @Test
    void skipLogFileWithoutHeader() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        life.add( logFiles );
        life.start();

        // simulate new file without header presence
        logVersionRepository.incrementAndGetVersion( NULL );
        fileSystem.write( logFiles.getLogFile().getLogFileForVersion( logVersionRepository.getCurrentLogVersion() ) ).close();
        transactionIdStore.transactionCommitted( 5L, 5, 5L, NULL );

        PhysicalLogicalTransactionStore.LogVersionLocator versionLocator = new PhysicalLogicalTransactionStore.LogVersionLocator( 4L );
        logFiles.getLogFile().accept( versionLocator );

        LogPosition logPosition = versionLocator.getLogPosition();
        assertEquals( 1, logPosition.getLogVersion() );
    }

    @Test
    void preAllocateOnStartAndEvictOnShutdownNewLogFile() throws IOException
    {
        final CapturingNativeAccess capturingNativeAccess = new CapturingNativeAccess();
        LogFilesBuilder.builder( databaseLayout, fileSystem )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .withStoreId( StoreId.UNKNOWN )
                .withNativeAccess( capturingNativeAccess )
                .build();

        startStop( capturingNativeAccess, life );

        assertEquals( 1, capturingNativeAccess.getPreallocateCounter() );
        assertEquals( 1, capturingNativeAccess.getEvictionCounter() );
        assertEquals( 0, capturingNativeAccess.getAdviseCounter() );
        assertEquals( 0, capturingNativeAccess.getKeepCounter() );
    }

    @Test
    void adviseOnStartAndEvictOnShutdownExistingLogFile() throws IOException
    {
        var capturingNativeAccess = new CapturingNativeAccess();

        startStop( capturingNativeAccess, life );
        capturingNativeAccess.reset();

        startStop( capturingNativeAccess, new LifeSupport() );

        assertEquals( 0, capturingNativeAccess.getPreallocateCounter() );
        assertEquals( 1, capturingNativeAccess.getEvictionCounter() );
        assertEquals( 1, capturingNativeAccess.getAdviseCounter() );
        assertEquals( 1, capturingNativeAccess.getKeepCounter() );
    }

    @Test
    void shouldOpenInFreshDirectoryAndFinallyAddHeader() throws Exception
    {
        // GIVEN
        LogFiles logFiles = buildLogFiles();

        // WHEN
        life.start();
        life.add( logFiles );
        life.shutdown();

        // THEN
        Path file =  LogFilesBuilder.logFilesBasedOnlyBuilder( databaseLayout.getTransactionLogsDirectory(), fileSystem )
                .withLogEntryReader( logEntryReader() )
                .build().getLogFile().getLogFileForVersion( 1L );
        LogHeader header = readLogHeader( fileSystem, file, INSTANCE );
        assertEquals( 1L, header.getLogVersion() );
        assertEquals( 2L, header.getLastCommittedTxId() );
    }

    @Test
    void shouldWriteSomeDataIntoTheLog() throws Exception
    {
        // GIVEN
        LogFiles logFiles = buildLogFiles();
        life.start();
        life.add( logFiles );

        // WHEN
        LogFile logFile = logFiles.getLogFile();
        TransactionLogWriter transactionLogWriter = logFile.getTransactionLogWriter();
        var channel = transactionLogWriter.getChannel();
        LogPosition currentPosition = transactionLogWriter.getCurrentPosition();
        int intValue = 45;
        long longValue = 4854587;
        channel.putInt( intValue );
        channel.putLong( longValue );
        logFile.flush();

        // THEN
        try ( ReadableChannel reader = logFile.getReader( currentPosition ) )
        {
            assertEquals( intValue, reader.getInt() );
            assertEquals( longValue, reader.getLong() );
        }
    }

    @Test
    void shouldReadOlderLogs() throws Exception
    {
        // GIVEN
        LogFiles logFiles = buildLogFiles();
        life.start();
        life.add( logFiles );

        // WHEN
        LogFile logFile = logFiles.getLogFile();
        TransactionLogWriter logWriter = logFile.getTransactionLogWriter();
        var writer = logWriter.getChannel();
        LogPosition position1 = logWriter.getCurrentPosition();
        int intValue = 45;
        long longValue = 4854587;
        byte[] someBytes = someBytes( 40 );
        writer.putInt( intValue );
        writer.putLong( longValue );
        writer.put( someBytes, someBytes.length );
        logFile.flush();
        LogPosition position2 = logWriter.getCurrentPosition();
        long longValue2 = 123456789L;
        writer.putLong( longValue2 );
        writer.put( someBytes, someBytes.length );
        logFile.flush();

        // THEN
        try ( ReadableChannel reader = logFile.getReader( position1 ) )
        {
            assertEquals( intValue, reader.getInt() );
            assertEquals( longValue, reader.getLong() );
            assertArrayEquals( someBytes, readBytes( reader, 40 ) );
        }
        try ( ReadableChannel reader = logFile.getReader( position2 ) )
        {
            assertEquals( longValue2, reader.getLong() );
            assertArrayEquals( someBytes, readBytes( reader, 40 ) );
        }
    }

    @Test
    void shouldVisitLogFile() throws Exception
    {
        // GIVEN
        LogFiles logFiles = buildLogFiles();
        life.start();
        life.add( logFiles );

        LogFile logFile = logFiles.getLogFile();
        var transactionLogWriter = logFile.getTransactionLogWriter();
        var writer = transactionLogWriter.getChannel();
        LogPosition position = transactionLogWriter.getCurrentPosition();
        for ( int i = 0; i < 5; i++ )
        {
            writer.put( (byte) i );
        }
        logFile.flush();

        // WHEN/THEN
        final AtomicBoolean called = new AtomicBoolean();
        logFile.accept( channel ->
        {
            for ( int i = 0; i < 5; i++ )
            {
                assertEquals( (byte)i, channel.get() );
            }
            called.set( true );
            return true;
        }, position );
        assertTrue( called.get() );
    }

    @Test
    void shouldCloseChannelInFailedAttemptToReadHeaderAfterOpen() throws Exception
    {
        // GIVEN a file which returns 1/2 log header size worth of bytes
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        int logVersion = 0;
        Path logFile = logFiles.getLogFile().getLogFileForVersion( logVersion );
        StoreChannel channel = mock( StoreChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( CURRENT_FORMAT_LOG_HEADER_SIZE / 2 );
        when( fs.fileExists( logFile ) ).thenReturn( true );
        when( fs.read( eq( logFile ) ) ).thenReturn( channel );

        // WHEN
        assertThrows( IncompleteLogHeaderException.class, () -> logFiles.getLogFile().openForVersion( logVersion ) );
        verify( channel ).close();
    }

    @Test
    void shouldSuppressFailureToCloseChannelInFailedAttemptToReadHeaderAfterOpen() throws Exception
    {
        // GIVEN a file which returns 1/2 log header size worth of bytes
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .build();
        int logVersion = 0;
        Path logFile = logFiles.getLogFile().getLogFileForVersion( logVersion );
        StoreChannel channel = mock( StoreChannel.class );
        when( channel.read( any( ByteBuffer.class ) ) ).thenReturn( CURRENT_FORMAT_LOG_HEADER_SIZE / 2 );
        when( fs.fileExists( logFile ) ).thenReturn( true );
        when( fs.read( eq( logFile ) ) ).thenReturn( channel );
        doThrow( IOException.class ).when( channel ).close();

        // WHEN
        IncompleteLogHeaderException exception =
                assertThrows( IncompleteLogHeaderException.class, () -> logFiles.getLogFile().openForVersion( logVersion ) );
        verify( channel ).close();
        assertEquals( 1, exception.getSuppressed().length );
        assertTrue( exception.getSuppressed()[0] instanceof IOException );
    }

    @Test
    void closeChannelThrowExceptionOnAttemptToAppendTransactionLogRecords() throws IOException
    {
        LogFiles logFiles = buildLogFiles();
        life.start();
        life.add( logFiles );

        LogFile logFile = logFiles.getLogFile();
        var channel = logFile.getTransactionLogWriter().getChannel();

        life.shutdown();

        assertThrows( Throwable.class, () -> channel.put( (byte) 7 ) );
        assertThrows( Throwable.class, () -> channel.putInt( 7 ) );
        assertThrows( Throwable.class, () -> channel.putLong( 7 ) );
        assertThrows( Throwable.class, () -> channel.putDouble( 7 ) );
        assertThrows( Throwable.class, () -> channel.putFloat( 7 ) );
        assertThrows( Throwable.class, () -> channel.putShort( (short) 7 ) );
        assertThrows( Throwable.class, () -> channel.put( new byte[]{1, 2, 3}, 3 ) );
        assertThrows( IllegalStateException.class, logFile::flush );
    }

    @Test
    void shouldForceLogChannel() throws Throwable
    {
        LogFiles logFiles = buildLogFiles();
        life.start();
        life.add( logFiles );

        LogFile logFile = logFiles.getLogFile();
        var capturingChannel = wrappingFileSystem.getCapturingChannel();

        var flushesBefore = capturingChannel.getFlushCounter().get();
        var writesBefore = capturingChannel.getWriteAllCounter().get();

        logFile.forceAfterAppend( LogAppendEvent.NULL );

        assertEquals( 1, capturingChannel.getFlushCounter().get() - flushesBefore );
        assertEquals( 1, capturingChannel.getWriteAllCounter().get() - writesBefore );
    }

        @Test
    void shouldBatchUpMultipleWaitingForceRequests() throws Throwable
    {
        LogFiles logFiles = buildLogFiles();
        life.start();
        life.add( logFiles );

        LogFile logFile = logFiles.getLogFile();
        var capturingChannel = wrappingFileSystem.getCapturingChannel();

        var flushesBefore = capturingChannel.getFlushCounter().get();
        var writesBefore = capturingChannel.getWriteAllCounter().get();
        ReentrantLock writeAllLock = capturingChannel.getWriteAllLock();
        writeAllLock.lock();

        int executors = 10;
        var executorService = Executors.newFixedThreadPool( executors );
        try
        {
            List<Future<?>> futures = Stream.iterate( 0, i -> i + 1 )
                    .limit( executors )
                    .map( v -> executorService.submit( () -> logFile.forceAfterAppend( LogAppendEvent.NULL ) ) )
                    .collect( toList() );
            while ( !writeAllLock.hasQueuedThreads() )
            {
                parkNanos( 100 );
            }
            writeAllLock.unlock();
            assertThat( futures ).hasSize( executors );
            Futures.getAll( futures );
        }
        finally
        {
            if ( writeAllLock.isLocked() )
            {
                writeAllLock.unlock();
            }
            executorService.shutdownNow();
        }
        assertThat( capturingChannel.getFlushCounter().get() - flushesBefore ).isLessThanOrEqualTo( executors );
        assertThat( capturingChannel.getWriteAllCounter().get() - writesBefore ).isLessThanOrEqualTo( executors );
    }

    @Test
    void shouldWaitForOngoingForceToCompleteBeforeForcingAgain() throws Throwable
    {
        LogFiles logFiles = buildLogFiles();
        life.start();
        life.add( logFiles );

        LogFile logFile = logFiles.getLogFile();
        var capturingChannel = wrappingFileSystem.getCapturingChannel();
        ReentrantLock writeAllLock = capturingChannel.getWriteAllLock();
        var flushesBefore = capturingChannel.getFlushCounter().get();
        var writesBefore = capturingChannel.getWriteAllCounter().get();
        writeAllLock.lock();

        int executors = 10;
        var executorService = Executors.newFixedThreadPool( executors );
        try
        {
            var future = executorService.submit( () -> logFile.forceAfterAppend( LogAppendEvent.NULL ) );
            while ( !writeAllLock.hasQueuedThreads() )
            {
                parkNanos( 100 );
            }
            writeAllLock.unlock();
            var future2 = executorService.submit( () -> logFile.forceAfterAppend( LogAppendEvent.NULL ) );
            Futures.getAll( List.of(future, future2 ) );
        }
        finally
        {
            if ( writeAllLock.isLocked() )
            {
                writeAllLock.unlock();
            }
            executorService.shutdownNow();
        }

        assertThat( capturingChannel.getWriteAllCounter().get() - writesBefore ).isEqualTo( 2 );
        assertThat( capturingChannel.getFlushCounter().get() - flushesBefore ).isEqualTo( 2 );
    }

    private static byte[] readBytes( ReadableChannel reader, int length ) throws IOException
    {
        byte[] result = new byte[length];
        reader.get( result, length );
        return result;
    }

    private LogFiles buildLogFiles() throws IOException
    {
        return LogFilesBuilder.builder( databaseLayout, wrappingFileSystem )
                .withRotationThreshold( rotationThreshold )
                .withTransactionIdStore( transactionIdStore )
                .withLogVersionRepository( logVersionRepository )
                .withLogEntryReader( logEntryReader() )
                .withStoreId( StoreId.UNKNOWN )
                .build();
    }

    private static byte[] someBytes( int length )
    {
        byte[] result = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            result[i] = (byte) (i % 5);
        }
        return result;
    }

    private void startStop( CapturingNativeAccess capturingNativeAccess, LifeSupport lifeSupport ) throws IOException
    {
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fileSystem )
                            .withTransactionIdStore( transactionIdStore )
                            .withLogVersionRepository( logVersionRepository )
                            .withLogEntryReader( logEntryReader() )
                            .withStoreId( StoreId.UNKNOWN )
                            .withNativeAccess( capturingNativeAccess ).build();

        lifeSupport.add( logFiles );
        lifeSupport.start();

        lifeSupport.shutdown();
    }

    private static class CapturingNativeAccess implements NativeAccess
    {
        private int evictionCounter;
        private int adviseCounter;
        private int preallocateCounter;
        private int keepCounter;

        @Override
        public boolean isAvailable()
        {
            return true;
        }

        @Override
        public NativeCallResult tryEvictFromCache( int fd )
        {
            evictionCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public NativeCallResult tryAdviseSequentialAccess( int fd )
        {
            adviseCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public NativeCallResult tryAdviseToKeepInCache( int fd )
        {
            keepCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public NativeCallResult tryPreallocateSpace( int fd, long bytes )
        {
            preallocateCounter++;
            return NativeCallResult.SUCCESS;
        }

        @Override
        public String describe()
        {
            return "Test only";
        }

        public int getEvictionCounter()
        {
            return evictionCounter;
        }

        public int getAdviseCounter()
        {
            return adviseCounter;
        }

        public int getKeepCounter()
        {
            return keepCounter;
        }

        public int getPreallocateCounter()
        {
            return preallocateCounter;
        }

        public void reset()
        {
            adviseCounter = 0;
            evictionCounter = 0;
            preallocateCounter = 0;
            keepCounter = 0;
        }
    }

    private class CapturingChannelFileSystem extends DelegatingFileSystemAbstraction
    {
        private CapturingStoreChannel capturingChannel;

        CapturingChannelFileSystem( FileSystemAbstraction fs )
        {
            super( fs );
        }

        @Override
        public StoreChannel write( Path fileName ) throws IOException
        {
            if ( fileName.toString().contains( TransactionLogFilesHelper.DEFAULT_NAME ) )
            {
                capturingChannel = new CapturingStoreChannel( super.write( fileName ) );
                return capturingChannel;
            }
            return super.write( fileName );
        }

        public CapturingStoreChannel getCapturingChannel()
        {
            return capturingChannel;
        }
    }

    private static class CapturingStoreChannel extends DelegatingStoreChannel
    {
        private final AtomicInteger writeAllCounter = new AtomicInteger();
        private final AtomicInteger flushCounter = new AtomicInteger();
        private final ReentrantLock writeAllLock = new ReentrantLock();

        private CapturingStoreChannel( StoreChannel delegate )
        {
            super( delegate );
        }

        @Override
        public void writeAll( ByteBuffer src ) throws IOException
        {
            writeAllLock.lock();
            try
            {
                writeAllCounter.incrementAndGet();
                super.writeAll( src );
            }
            finally
            {
                writeAllLock.unlock();
            }
        }

        @Override
        public void flush() throws IOException
        {
            flushCounter.incrementAndGet();
            super.flush();
        }

        public ReentrantLock getWriteAllLock()
        {
            return writeAllLock;
        }

        public AtomicInteger getWriteAllCounter()
        {
            return writeAllCounter;
        }

        public AtomicInteger getFlushCounter()
        {
            return flushCounter;
        }
    }
}
