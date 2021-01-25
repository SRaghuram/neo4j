/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl;

import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.time.Stopwatch;
import org.neo4j.time.SystemNanoClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.internal.helpers.Format.duration;

/**
 * Monitor for events that should be displayed to neo4j-admin backup stdout
 */
public class BackupOutputMonitor implements StoreCopyClientMonitor
{
    private final Log log;
    private final SystemNanoClock clock;
    private Stopwatch startTime;
    private Stopwatch partStartTime;

    public BackupOutputMonitor( LogProvider logProvider, SystemNanoClock clock )
    {
        log = logProvider.getLog( getClass() );
        this.clock = clock;
    }

    @Override
    public void start()
    {
        startTime = clock.startStopWatch();
    }

    @Override
    public void startReceivingStoreFiles()
    {
        log.info( "Start receiving store files" );
        notePartStartTime();
    }

    @Override
    public void finishReceivingStoreFiles()
    {
        log.info( "Finish receiving store files, took %s", durationSincePartStartTime() );
    }

    @Override
    public void startReceivingStoreFile( String file )
    {
        log.info( "Start receiving store file %s", file );
    }

    @Override
    public void finishReceivingStoreFile( String file )
    {
        log.info( "Finish receiving store file %s", file );
    }

    @Override
    public void startReceivingTransactions( long startTxId )
    {
        log.info( "Start receiving transactions from %d", startTxId );
        notePartStartTime();
    }

    @Override
    public void finishReceivingTransactions( long endTxId )
    {
        log.info( "Finish receiving transactions at %d, took %s", endTxId, durationSincePartStartTime() );
    }

    @Override
    public void startRecoveringStore()
    {
        log.info( "Start recovering store" );
        notePartStartTime();
    }

    @Override
    public void finishRecoveringStore()
    {
        log.info( "Finish recovering store, took %s", durationSincePartStartTime() );
    }

    @Override
    public void startReceivingIndexSnapshots()
    {
        log.info( "Start receiving index snapshots" );
        notePartStartTime();
    }

    @Override
    public void startReceivingIndexSnapshot( long indexId )
    {
        log.info( "Start receiving index snapshot id %d", indexId );
    }

    @Override
    public void finishReceivingIndexSnapshot( long indexId )
    {
        log.info( "Finished receiving index snapshot id %d", indexId );
    }

    @Override
    public void finishReceivingIndexSnapshots()
    {
        log.info( "Finished receiving index snapshots, took %s", durationSincePartStartTime() );
    }

    @Override
    public void finish()
    {
        log.info( "Finished, took %s", duration( startTime.elapsed( MILLISECONDS ) ) );
    }

    private void notePartStartTime()
    {
        partStartTime = clock.startStopWatch();
    }

    private String durationSincePartStartTime()
    {
        return duration( partStartTime.elapsed( MILLISECONDS ) );
    }
}
