/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.api.TestCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.SimpleLogVersionRepository;
import org.neo4j.kernel.impl.transaction.SimpleTransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.OtherThreadRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests an issue where writer would append data and sometimes rotate the log to new file. When rotating the log
 * there's an intricate relationship between {@link LogVersionRepository}, creating the file and writing
 * the header. Concurrent readers which scans the log stream will use {@link LogVersionBridge} to seemlessly
 * jump over to new files, where the highest file is dictated by {@link LogVersionRepository#getCurrentLogVersion()}.
 * There was this race where the log version was incremented, the new log file created and reader would get
 * to this new file and try to read the header and fail before the header had been written.
 * <p>
 * This test tries to reproduce this race. It will not produce false negatives, but sometimes false positives
 * since it's non-deterministic.
 */
@Neo4jLayoutExtension
@ExtendWith( LifeExtension.class )
class TransactionLogFileRotateAndReadRaceIT
{
    @Inject
    private LifeSupport life;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private DatabaseLayout databaseLayout;

    private final OtherThreadRule<Void> t2 = new OtherThreadRule<>();

    // If any of these limits are reached the test ends, that or if there's a failure of course
    private static final long LIMIT_TIME = SECONDS.toMillis( 5 );
    private static final int LIMIT_ROTATIONS = 500;
    private static final int LIMIT_READS = 1_000;

    @BeforeEach
    void setUp()
    {
        t2.init( getClass().getName() + "-T2" );
    }

    @AfterEach
    void tearDown()
    {
        t2.close();
    }

    @Test
    void shouldNotSeeEmptyLogFileWhenReadingTransactionStream() throws Exception
    {
        // GIVEN
        LogVersionRepository logVersionRepository = new SimpleLogVersionRepository();
        LogFiles logFiles = LogFilesBuilder.builder( databaseLayout, fs )
                .withLogVersionRepository( logVersionRepository )
                .withTransactionIdStore( new SimpleTransactionIdStore() )
                .withLogEntryReader( new VersionAwareLogEntryReader( new TestCommandReaderFactory() ) )
                .withStoreId( StoreId.UNKNOWN )
                .build();
        life.add( logFiles );
        LogFile logFile = logFiles.getLogFile();
        FlushablePositionAwareChecksumChannel writer = logFile.getWriter();
        LogPositionMarker startPosition = new LogPositionMarker();
        writer.getCurrentPosition( startPosition );

        // WHEN
        AtomicBoolean end = new AtomicBoolean();
        byte[] dataChunk = new byte[100];
        // one thread constantly writing to and rotating the channel
        AtomicInteger rotations = new AtomicInteger();
        CountDownLatch startSignal = new CountDownLatch( 1 );
        long maxEndTime = currentTimeMillis() + LIMIT_TIME;
        Future<Void> writeFuture = t2.execute( ignored ->
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            startSignal.countDown();
            while ( !end.get() && (currentTimeMillis() < maxEndTime) )
            {
                writer.put( dataChunk, random.nextInt( 1, dataChunk.length ) );
                if ( logFile.rotationNeeded() )
                {
                    logFile.rotate();
                    // Let's just close the gap to the reader so that it gets closer to the "hot zone"
                    // where the rotation happens.
                    writer.getCurrentPosition( startPosition );
                    rotations.incrementAndGet();
                }
            }
            return null;
        } );
        assertTrue( startSignal.await( 10, SECONDS ) );
        // one thread reading through the channel
        int reads = 0;
        try
        {
            for ( ; currentTimeMillis() < maxEndTime &&
                reads < LIMIT_READS &&
                rotations.get() < LIMIT_ROTATIONS; reads++ )
            {
                try ( ReadableLogChannel reader = logFile.getReader( startPosition.newPosition() ) )
                {
                    deplete( reader );
                }
            }
        }
        finally
        {
            end.set( true );
            writeFuture.get();
        }

        // THEN simply getting here means this was successful
    }

    private static void deplete( ReadableLogChannel reader )
    {
        byte[] dataChunk = new byte[100];
        try
        {
            while ( true )
            {
                reader.get( dataChunk, dataChunk.length );
            }
        }
        catch ( ReadPastEndException e )
        {
            // This is OK, it means we've reached the end
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }
}
