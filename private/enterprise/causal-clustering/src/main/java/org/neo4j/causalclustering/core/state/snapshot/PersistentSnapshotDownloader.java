/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.snapshot;

import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.storecopy.DatabaseShutdownException;
import org.neo4j.causalclustering.core.state.CommandApplicationProcess;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;

public class PersistentSnapshotDownloader implements Runnable
{
    public interface Monitor
    {
        void startedDownloadingSnapshot();

        void downloadSnapshotComplete();
    }

    static final String OPERATION_NAME = "download of snapshot";

    private final CommandApplicationProcess applicationProcess;
    private final CatchupAddressProvider addressProvider;
    private final CoreStateDownloader downloader;
    private final Log log;
    private final TimeoutStrategy.Timeout timeout;
    private final Supplier<DatabaseHealth> dbHealth;
    private final Monitor monitor;
    private volatile State state;
    private volatile boolean keepRunning;

    PersistentSnapshotDownloader( CatchupAddressProvider addressProvider, CommandApplicationProcess applicationProcess,
            CoreStateDownloader downloader, Log log, TimeoutStrategy.Timeout pauseStrategy,
            Supplier<DatabaseHealth> dbHealth, Monitors monitors )
    {
        this.applicationProcess = applicationProcess;
        this.addressProvider = addressProvider;
        this.downloader = downloader;
        this.log = log;
        this.timeout = pauseStrategy;
        this.dbHealth = dbHealth;
        this.monitor = monitors.newMonitor( Monitor.class );
        this.state = State.INITIATED;
        this.keepRunning = true;
    }

    private enum State
    {
        INITIATED,
        RUNNING,
        COMPLETED
    }

    @Override
    public void run()
    {
        if ( !moveToRunningState() )
        {
            return;
        }

        try
        {
            monitor.startedDownloadingSnapshot();
            applicationProcess.pauseApplier( OPERATION_NAME );
            while ( keepRunning && !downloader.downloadSnapshot( addressProvider ) )
            {
                Thread.sleep( timeout.getMillis() );
                timeout.increment();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            log.warn( "Persistent snapshot downloader was interrupted" );
        }
        catch ( DatabaseShutdownException e )
        {
            log.warn( "Store copy aborted due to shut down", e );
        }
        catch ( Throwable e )
        {
            log.error( "Unrecoverable error during store copy", e );
            dbHealth.get().panic( e );
        }
        finally
        {
            applicationProcess.resumeApplier( OPERATION_NAME );
            monitor.downloadSnapshotComplete();
            state = State.COMPLETED;
        }
    }

    private synchronized boolean moveToRunningState()
    {
        if ( state != State.INITIATED )
        {
            return false;
        }
        else
        {
            state = State.RUNNING;
            return true;
        }
    }

    void stop() throws InterruptedException
    {
        this.keepRunning = false;

        while ( !hasCompleted() )
        {
            Thread.sleep( 100 );
        }
    }

    boolean hasCompleted()
    {
        return state == State.COMPLETED;
    }
}
