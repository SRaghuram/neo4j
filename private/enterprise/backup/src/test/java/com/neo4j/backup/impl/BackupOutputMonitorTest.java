/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.time.FakeClock;

import static org.hamcrest.Matchers.containsString;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class BackupOutputMonitorTest
{
    private final Monitors monitors = new Monitors();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Test
    void receivingStoreFilesMessageCorrect()
    {
        // given
        Monitors monitors = new Monitors();
        AssertableLogProvider logProvider = new AssertableLogProvider();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider ) );

        // when
        StoreCopyClientMonitor storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();

        // then
        logProvider.assertAtLeastOnce( inLog( BackupOutputMonitor.class ).info( containsString( "Start receiving store files" ) ) );
    }

    @Test
    void shouldIncludeDurationOfReceivingStoreFiles()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.startReceivingStoreFiles();
        monitor.startReceivingStoreFile( "some file" );
        monitor.finishReceivingStoreFile( "some file" );
        clock.forward( 3, TimeUnit.SECONDS );
        monitor.finishReceivingStoreFiles();

        // then
        logProvider.formattedMessageMatcher().assertContains( "Finish receiving store files, took 3s" );
    }

    @Test
    void shouldIncludeDurationOfReceivingTransactions()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        long endTxId = 10;
        monitor.startReceivingTransactions( 1 );
        clock.forward( 10_500, TimeUnit.MILLISECONDS );
        monitor.finishReceivingTransactions( endTxId );

        // then
        logProvider.formattedMessageMatcher().assertContains( "Finish receiving transactions at " + endTxId + ", took 10s 500ms" );
    }

    @Test
    void shouldIncludeDurationOfRecoveringStore()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.startRecoveringStore();
        clock.forward( 1, TimeUnit.SECONDS );
        monitor.finishRecoveringStore();

        // then
        logProvider.formattedMessageMatcher().assertContains( "Finish recovering store, took 1s" );
    }

    @Test
    void shouldIncludeDurationOfReceivingIndexSnapshots()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.startReceivingIndexSnapshots();
        monitor.startReceivingIndexSnapshot( 1 );
        monitor.finishReceivingIndexSnapshot( 1 );
        clock.forward( 2, TimeUnit.SECONDS );
        monitor.finishReceivingIndexSnapshots();

        // then
        logProvider.formattedMessageMatcher().assertContains( "Finished receiving index snapshots, took 2s" );
    }

    @Test
    void shouldIncludeDurationOfFinished()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.start();
        clock.forward( 5, TimeUnit.SECONDS );
        monitor.finish();

        // then
        logProvider.formattedMessageMatcher().assertContains( "Finished, took 5s" );
    }

    @Test
    void shouldIncludeDurationOfFinishedAndForParts()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( logProvider, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.start();
        monitor.startReceivingStoreFiles();
        clock.forward( 5, TimeUnit.SECONDS );
        monitor.finishReceivingStoreFiles();
        clock.forward( 2_500, TimeUnit.MILLISECONDS );
        monitor.finish();

        // then
        logProvider.formattedMessageMatcher().assertContains( "Finish receiving store files, took 5s" );
        logProvider.formattedMessageMatcher().assertContains( "Finished, took 7s 500ms" );
    }
}
