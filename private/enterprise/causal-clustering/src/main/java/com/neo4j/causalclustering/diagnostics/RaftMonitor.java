/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.PersistentSnapshotDownloader;
import com.neo4j.causalclustering.helper.Limiters;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;

import java.time.Duration;
import java.util.function.Consumer;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;

import static java.lang.String.format;

/**
 * Monitors major raft events and logs them appropriately. The main intention
 * is for this class to make sure that the neo4j.log gets the most important events
 * logged in a way that is useful for end users and aligned across components.
 * <p>
 * In particular the startup should be logged in a way as to aid in debugging
 * common issues; e.g. around network connectivity.
 * <p>
 * This pattern also de-clutters implementing classes from specifics of logging (e.g.
 * formatting, dual-logging, rate limiting, ...) and encourages a structured interface.
 */
public class RaftMonitor implements RaftBinder.Monitor, PersistentSnapshotDownloader.Monitor
{
    private final Log debug;
    private final Log user;

    private final Consumer<Runnable> binderLimit = Limiters.rateLimiter( Duration.ofSeconds( 10 ) );

    public static void register( LogService logService, Monitors monitors )
    {
        var raftMonitor = new RaftMonitor( logService );
        monitors.addMonitorListener( raftMonitor );
    }

    private RaftMonitor( LogService logService )
    {
        this.debug = logService.getInternalLogProvider().getLog( getClass() );
        this.user = logService.getUserLogProvider().getLog( getClass() );
    }

    @Override
    public void waitingForCoreMembers( DatabaseId databaseId, int minimumCount )
    {
        binderLimit.accept( () -> user.info( "Database '%s' is waiting for a total of %d core members...", databaseId.name(), minimumCount ) );
    }

    @Override
    public void waitingForBootstrap( DatabaseId databaseId )
    {
        binderLimit.accept( () -> user.info( "Database '%s' is waiting for bootstrap by other instance...", databaseId.name() ) );
    }

    @Override
    public void bootstrapped( CoreSnapshot snapshot, DatabaseId databaseId, RaftId raftId )
    {
        user.info( format( "This instance bootstrapped a raft for database '%s'.", databaseId.name() ) );
        debug.info( format( "Bootstrapped %s with %s using %s", databaseId, raftId, snapshot ) );
    }

    @Override
    public void boundToRaft( DatabaseId databaseId, RaftId raftId )
    {
        user.info( format( "Bound database '%s' to raft with id '%s'.", databaseId.name(), raftId.uuid() ) );
    }

    @Override
    public void startedDownloadingSnapshot( DatabaseId databaseId )
    {
        user.info( "Started downloading snapshot for database '%s'...", databaseId.name() );
    }

    @Override
    public void downloadSnapshotComplete( DatabaseId databaseId )
    {
        user.info( "Download of snapshot for database '%s' complete.", databaseId.name() );
    }
}
