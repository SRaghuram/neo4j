/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.io.PrintStream;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.AssertableLogProvider.Level;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.FakeClockJobScheduler;

import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

class TxPullRequestBatchingLoggerTest
{
    private static final Class<?> LOGGING_CLASS = TxPullRequestBatchingLogger.class;
    private static final Duration BATCH_TIME = Duration.ofMinutes( 5 );

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private TxPullRequestBatchingLogger txPullRqLogger;
    private FakeClockJobScheduler scheduler;

    @BeforeEach
    void setUp()
    {
        logProvider.clear();
        scheduler = new FakeClockJobScheduler();
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, scheduler, BATCH_TIME );
        txPullRqLogger.start();
    }

    @Test
    void shouldLogNewEntriesOnceOnly()
    {
        // given
        var remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId = 42;
        var dbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );

        // when a TxPullRequest is first received
        txPullRqLogger.onTxPullRequest( socketAddress, msg );

        // then it will be logged immediately
        var expectedMessage = newEntryMessage( socketAddress, dbId, prevTxId );
        assertThat( logProvider )
                .forClass( LOGGING_CLASS )
                .forLevel( Level.INFO )
                .containsMessages( expectedMessage );

        // when same txPullRequest is logged again
        logProvider.clear();
        txPullRqLogger.onTxPullRequest( socketAddress, msg );

        // then no new log entry shall be made
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // when same txPullRequest only changed previousTransactionId
        msg = new TxPullRequest( prevTxId + 5, StoreId.UNKNOWN, dbId );
        txPullRqLogger.onTxPullRequest( socketAddress, msg );

        // then no new log entry shall be made
        assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void shouldOnlyLogSimpleIfStopped()
    {
        // Given txPullRqLogger is not in a started state
        txPullRqLogger.stop();

        // When entries are submitted
        var remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId = 42;
        var dbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );

        txPullRqLogger.onTxPullRequest( socketAddress, msg );

        // Then they should be logged immediately
        var expectedMessage = format( "Handling %s [From: %s]", msg.describe(), remoteHost );
        assertThat( logProvider )
                .forClass( LOGGING_CLASS )
                .forLevel( Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldNotFlushAnythingIfStopped()
    {
        // Given txPullRqLogger is not in a started state but receives log submissions
        txPullRqLogger.stop();

        var remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId = 42;
        var dbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );

        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        logProvider.clear();

        // When flush is trigger
        scheduler.forward( BATCH_TIME.plusSeconds( 1 ) );

        // Then nothing should be logged
        assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void shouldLogAllNewEntries()
    {
        // Given two contexts/hosts and two databases
        var remoteHost1 = "testHost:1337";
        var remoteHost2 = "testHost:1338";
        var socketAddress1 = mockAddress( remoteHost1 );
        var socketAddress2 = mockAddress( remoteHost2 );
        var addresses = List.of( socketAddress1, socketAddress2 );

        var dbId1 = DatabaseIdFactory.from( UUID.randomUUID() );
        var dbId2 = DatabaseIdFactory.from( UUID.randomUUID() );
        var databases = List.of( dbId1, dbId2 );

        var prevTxId = 42;

        // Then all four combinations of database and host shall be logged
        addresses.forEach( address -> databases.forEach( db ->
                                                    {
                                                        logProvider.clear();

                                                        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, db );
                                                        txPullRqLogger.onTxPullRequest( address, msg );

                                                        var expectedMessage = newEntryMessage( address, db, prevTxId );

                                                        assertThat( logProvider )
                                                                .forClass( LOGGING_CLASS )
                                                                .forLevel( Level.INFO )
                                                                .containsMessages( expectedMessage );
                                                    } ) );
    }

    @Test
    void shouldBatchSimilarEntriesWithDifferentTxIds()
    {
        // Given batching is started
        var remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId = 42;
        var dbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );

        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        logProvider.clear();

        // When new entries are submitted
        var nbrOfMsg = 300;
        for ( int i = 1; i <= nbrOfMsg; i++ )
        {
            msg = new TxPullRequest( prevTxId + i, StoreId.UNKNOWN, dbId );
            txPullRqLogger.onTxPullRequest( socketAddress, msg );
        }

        // Then nothing should be logged
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // When batch time has elapsed
        scheduler.forward( BATCH_TIME.plusSeconds( 1 ) );

        // Then a batched log entry should be flushed
        var expectedMessage = format( "TxPullRequests batched latest %s:%n" +
                                      "    [%s] sent %d TxPullRequests for %s for transactions above id %d to %d",
                                      txPullRqLogger.batchTimeToString(),
                                      remoteHost,
                                      nbrOfMsg,
                                      dbId,
                                      prevTxId + 1,
                                      prevTxId + nbrOfMsg );
        assertThat( logProvider )
                .forClass( LOGGING_CLASS )
                .forLevel( Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldBatchSimilarEntriesWithSameTxId()
    {
        // Given batching is started
        var remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId = 42;
        var dbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );

        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        logProvider.clear();

        // When new entries are submitted
        var nbrOfMsg = 300;
        for ( int i = 1; i <= nbrOfMsg; i++ )
        {
            msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );
            txPullRqLogger.onTxPullRequest( socketAddress, msg );
        }

        // Then nothing should be logged
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // When batch time has elapsed
        scheduler.forward( BATCH_TIME.plusSeconds( 1 ) );

        // Then a batched log entry should be flushed
        var expectedMessage = format( "TxPullRequests batched latest %s:%n" +
                                      "    [%s] sent %d TxPullRequests for %s for transaction above id %d",
                                      txPullRqLogger.batchTimeToString(),
                                      remoteHost,
                                      nbrOfMsg,
                                      dbId,
                                      prevTxId );
        assertThat( logProvider )
                .forClass( LOGGING_CLASS )
                .forLevel( Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldNotBatchIfNoEntriesDuringPreviousBatchTime()
    {
        // Given an entry is being batched
        String remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId = 42;
        var dbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );

        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        logProvider.clear();

        // When a full batch time has elapsed without entries
        scheduler.forward( BATCH_TIME.plusSeconds( 1 ) );
        assertThat( logProvider ).doesNotHaveAnyLogs();

        // Then a new entry with same address and database is considered as new batching entry
        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        var expectedMessage = newEntryMessage( socketAddress, dbId, prevTxId );
        assertThat( logProvider )
                .forClass( LOGGING_CLASS )
                .forLevel( Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldResetCounterBetweenFlushes()
    {
        // Given batching is in progress
        var remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId = 42;
        var dbId = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg = new TxPullRequest( prevTxId, StoreId.UNKNOWN, dbId );

        txPullRqLogger.onTxPullRequest( socketAddress, msg );

        // When a log is flushed with some batch
        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        scheduler.forward( BATCH_TIME.plusSeconds( 1 ) );

        // Then the next log flush should have rest counters
        logProvider.clear();
        txPullRqLogger.onTxPullRequest( socketAddress, msg );
        scheduler.forward( BATCH_TIME );

        var expectedMessage = format( "TxPullRequests batched latest %s:%n" +
                                      "    [%s] sent 1 TxPullRequests for %s for transaction above id %d",
                                      txPullRqLogger.batchTimeToString(),
                                      remoteHost,
                                      dbId,
                                      prevTxId );
        assertThat( logProvider )
                .forClass( LOGGING_CLASS )
                .forLevel( Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldPruneBatchResultPerDatabase()
    {
        // Given batching two databases from one database
        var remoteHost = "testHost:1337";
        var socketAddress = mockAddress( remoteHost );

        var prevTxId1 = 42;
        var prevTxId2 = 193;
        var db1 = DatabaseIdFactory.from( UUID.randomUUID() );
        var db2 = DatabaseIdFactory.from( UUID.randomUUID() );
        var msg1 = new TxPullRequest( prevTxId1, StoreId.UNKNOWN, db1 );
        var msg2 = new TxPullRequest( prevTxId2, StoreId.UNKNOWN, db2 );

        txPullRqLogger.onTxPullRequest( socketAddress, msg1 );
        txPullRqLogger.onTxPullRequest( socketAddress, msg2 );
        logProvider.clear();

        // When no requests for one database, but still for the other
        scheduler.forward( BATCH_TIME.dividedBy( 2 ) );
        txPullRqLogger.onTxPullRequest( socketAddress, msg1 );
        scheduler.forward( BATCH_TIME );

        // Then only one database should be pruned
        var expectedMessage = format( "TxPullRequests batched latest %s:%n" +
                                      "    [%s] sent 1 TxPullRequests for %s for transaction above id %d",
                                      txPullRqLogger.batchTimeToString(),
                                      remoteHost,
                                      db1,
                                      prevTxId1 );
        assertThat( logProvider )
                .forClass( LOGGING_CLASS )
                .forLevel( Level.INFO )
                .containsMessages( expectedMessage );
    }

    @Test
    void shouldOutputNothingIfBatchingIsEmpty()
    {
        // Given only one log submit per database and address
        var addresses = new SocketAddress[3];
        var prevTxId = 42;
        var messages = new TxPullRequest[3];

        var multiplier = 3;
        for ( int i = 0; i < multiplier; i++ )
        {
            var remoteHost = "testHost:133" + i;
            addresses[i] = mockAddress( remoteHost );
            messages[i] = new TxPullRequest( prevTxId, StoreId.UNKNOWN, DatabaseIdFactory.from( UUID.randomUUID() ) );
        }

        forEachContextMessagePair( addresses, messages, txPullRqLogger::onTxPullRequest );
        logProvider.clear();

        // When batch time has elapsed
        scheduler.forward( BATCH_TIME.plusSeconds( 1 ) );

        // Then nothing should be flushed to log
        assertThat( logProvider ).doesNotHaveAnyLogs();
    }

    @Test
    void shouldBatchOnRemoteAddressAndDatabase()
    {
        // Given batching is started for multiple remote addresses and multiple databases
        var addresses = new SocketAddress[3];
        var prevTxId = 42;
        var messages = new TxPullRequest[3];

        var replicationFactor = 3;
        for ( int i = 0; i < replicationFactor; i++ )
        {
            var remoteHost = "testHost:133" + i;
            addresses[i] = mockAddress( remoteHost );
            messages[i] = new TxPullRequest( prevTxId, StoreId.UNKNOWN, DatabaseIdFactory.from( UUID.randomUUID() ) );
        }

        forEachContextMessagePair( addresses, messages, txPullRqLogger::onTxPullRequest );
        logProvider.clear();

        for ( int i = 0; i < 100; i++ )
        {
            forEachContextMessagePair( addresses, messages, txPullRqLogger::onTxPullRequest );
        }

        // When log is flushed
        scheduler.forward( BATCH_TIME.plusSeconds( 1 ) );

        // Then output should be one line per address with all databases on each line
        var logPrintSpy = new LogPrintSpy();
        logProvider.print( logPrintSpy );
        var logOutput = logPrintSpy.latestPrint;
        var expectedOutputPattern = format( "INFO @ %s: TxPullRequests batched latest " +
                                           "[0-9 a-z]+:(%n {4}\\[[a-zA-Z0-9:]+] sent ([0-9]+ TxPullRequests for DatabaseId\\{[a-z0-9]+} for transaction " +
                                           "above id [0-9]+(, )?){%d}){%d}", LOGGING_CLASS.getCanonicalName(), replicationFactor, replicationFactor );
        assertThat( logOutput.matches( expectedOutputPattern ) )
                .withFailMessage( "Log output:%n%s%nDoes not match pattern:%n%s", logOutput, expectedOutputPattern )
                .isTrue();
    }

    void forEachContextMessagePair( SocketAddress[] addresses, TxPullRequest[] messages, BiConsumer<SocketAddress,TxPullRequest> f )
    {
        Arrays.stream( addresses )
              .forEach( address -> Arrays.stream( messages )
                                     .forEach( msg -> f.accept( address, msg ) ) );
    }

    @Test
    void batchTimeToStringTest()
    {
        // Even hours
        var txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofSeconds( 7200 ) );
        var expectedString = "2 hours";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );

        // One hour
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofSeconds( 3600 ) );
        expectedString = "1 hour";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );

        // Even minutes
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofSeconds( 300 ) );
        expectedString = "5 minutes";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );

        // One minute
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofSeconds( 60 ) );
        expectedString = "1 minute";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );

        // Even seconds
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofSeconds( 65 ) );
        expectedString = "65 seconds";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );

        // One second
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofSeconds( 1 ) );
        expectedString = "1 second";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );

        // Less than 1 second
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofMillis( 700 ) );
        expectedString = "700 milliseconds";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );

        // One ms
        txPullRqLogger = new TxPullRequestBatchingLogger( logProvider, null, Duration.ofMillis( 1 ) );
        expectedString = "1 millisecond";
        assertThat( txPullRqLogger.batchTimeToString() ).isEqualTo( expectedString );
    }

    private String newEntryMessage( SocketAddress remoteHost, DatabaseId dbId, long prevTxId )
    {
        return format( "New TxPullRequest from %s for %s (above transaction id: %d)." +
                       " TxPullRequests for this database and address will be batched the next 5 minutes",
                       remoteHost, dbId, prevTxId );
    }

    private SocketAddress mockAddress( String host )
    {
        var address = mock( SocketAddress.class );
        when( address.toString() ).thenReturn( host );
        return address;
    }

    private static class LogPrintSpy extends PrintStream
    {
        String latestPrint;

        LogPrintSpy()
        {
            super( mock( OutputStream.class ) );
        }

        @Override
        public void println( String s )
        {
            latestPrint = s;
        }
    }
}
