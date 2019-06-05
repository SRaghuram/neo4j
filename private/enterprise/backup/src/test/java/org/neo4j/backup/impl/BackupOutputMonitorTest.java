/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.ParameterisedOutsideWorld;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.time.FakeClock;

import static org.junit.Assert.assertTrue;

public class BackupOutputMonitorTest
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private final Monitors monitors = new Monitors();
    private OutsideWorld outsideWorld;

    @Before
    public void setup()
    {
        outsideWorld = new ParameterisedOutsideWorld( System.console(), System.out, System.err, System.in, new DefaultFileSystemAbstraction() );
    }

    @Test
    public void receivingStoreFilesMessageCorrect()
    {
        // given
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld ) );

        // when
        StoreCopyClientMonitor storeCopyClientMonitor = monitors.newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();

        // then
        assertTrue( suppressOutput.getOutputVoice().toString().contains( "Start receiving store files" ) );
    }

    @Test
    public void shouldIncludeDurationOfReceivingStoreFiles()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.startReceivingStoreFiles();
        monitor.startReceivingStoreFile( "some file" );
        monitor.finishReceivingStoreFile( "some file" );
        clock.forward( 3, TimeUnit.SECONDS );
        monitor.finishReceivingStoreFiles();

        // then
        assertTrue( suppressOutput.getOutputVoice().toString().contains( "Finish receiving store files, took 3s" ) );
    }

    @Test
    public void shouldIncludeDurationOfReceivingTransactions()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        long endTxId = 10;
        monitor.startReceivingTransactions( 1 );
        clock.forward( 10_500, TimeUnit.MILLISECONDS );
        monitor.finishReceivingTransactions( endTxId );

        // then
        assertTrue( suppressOutput.getOutputVoice().toString().contains( "Finish receiving transactions at " + endTxId + ", took 10s 500ms" ) );
    }

    @Test
    public void shouldIncludeDurationOfRecoveringStore()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.startRecoveringStore();
        clock.forward( 1, TimeUnit.SECONDS );
        monitor.finishRecoveringStore();

        // then
        assertTrue( suppressOutput.getOutputVoice().toString().contains( "Finish recovering store, took 1s" ) );
    }

    @Test
    public void shouldIncludeDurationOfReceivingIndexSnapshots()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.startReceivingIndexSnapshots();
        monitor.startReceivingIndexSnapshot( 1 );
        monitor.finishReceivingIndexSnapshot( 1 );
        clock.forward( 2, TimeUnit.SECONDS );
        monitor.finishReceivingIndexSnapshots();

        // then
        assertTrue( suppressOutput.getOutputVoice().toString().contains( "Finished receiving index snapshots, took 2s" ) );
    }

    @Test
    public void shouldIncludeDurationOfFinished()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.start();
        clock.forward( 5, TimeUnit.SECONDS );
        monitor.finish();

        // then
        assertTrue( suppressOutput.getOutputVoice().toString().contains( "Finished, took 5s" ) );
    }

    @Test
    public void shouldIncludeDurationOfFinishedAndForParts()
    {
        // given
        FakeClock clock = new FakeClock();
        monitors.addMonitorListener( new BackupOutputMonitor( outsideWorld, clock ) );
        StoreCopyClientMonitor monitor = monitors.newMonitor( StoreCopyClientMonitor.class );

        // when
        monitor.start();
        monitor.startReceivingStoreFiles();
        clock.forward( 5, TimeUnit.SECONDS );
        monitor.finishReceivingStoreFiles();
        clock.forward( 2_500, TimeUnit.MILLISECONDS );
        monitor.finish();

        // then
        String output = suppressOutput.getOutputVoice().toString();
        assertTrue( output.contains( "Finish receiving store files, took 5s" ) );
        assertTrue( output.contains( "Finished, took 7s 500ms" ) );
    }
}
