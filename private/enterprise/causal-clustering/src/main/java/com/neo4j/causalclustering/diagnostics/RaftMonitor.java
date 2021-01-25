/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.diagnostics;

import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotMonitor;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.time.Clock;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.CappedLogger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

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
public class RaftMonitor implements RaftBinder.Monitor, CoreSnapshotMonitor
{
    private final Log debug;
    private final Log user;

    private final CappedLogger coreMemberWaitLog;
    private final CappedLogger bootstrapWaitLog;
    private final CappedLogger publishRaftIdLog;
    private final CappedLogger discoveryServiceAttemptLog;
    private final CappedLogger initialServersAttemptLog;

    public static void register( LogService logService, Monitors monitors, Clock clock )
    {
        var raftMonitor = new RaftMonitor( logService, clock );
        monitors.addMonitorListener( raftMonitor );
    }

    private RaftMonitor( LogService logService, Clock clock )
    {
        this.debug = logService.getInternalLogProvider().getLog( getClass() );
        this.user = logService.getUserLogProvider().getLog( getClass() );

        coreMemberWaitLog = new CappedLogger( user, 10, TimeUnit.SECONDS, clock );
        bootstrapWaitLog = new CappedLogger( user, 10, TimeUnit.SECONDS, clock );

        publishRaftIdLog = new CappedLogger( debug, 5, TimeUnit.SECONDS, clock );
        discoveryServiceAttemptLog = new CappedLogger( debug, 10, TimeUnit.SECONDS, clock );
        initialServersAttemptLog = new CappedLogger( debug, 10, TimeUnit.SECONDS, clock );
    }

    @Override
    public void waitingForCoreMembers( NamedDatabaseId namedDatabaseId, int minimumCount )
    {
        coreMemberWaitLog.info( "Database '%s' is waiting for a total of %d core members...", namedDatabaseId.name(), minimumCount );
    }

    @Override
    public void waitingForBootstrap( NamedDatabaseId namedDatabaseId )
    {
        bootstrapWaitLog.info( "Database '%s' is waiting for bootstrap by other instance...", namedDatabaseId.name() );
    }

    @Override
    public void bootstrapped( CoreSnapshot snapshot, NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId )
    {
        user.info( format( "This instance bootstrapped the '%s' database.", namedDatabaseId.name() ) );
        debug.info( format( "Bootstrapped %s using %s as %s", namedDatabaseId, snapshot, raftMemberId ) );
    }

    @Override
    public void boundToRaftFromDisk( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId )
    {
        debug.info( format( "Bound as %s to %s existing on disk", raftMemberId, namedDatabaseId ) );
    }

    @Override
    public void boundToRaftThroughTopology( NamedDatabaseId namedDatabaseId, RaftMemberId raftMemberId )
    {
        debug.info( format( "Bound as %s to %s published in discovery", raftMemberId, namedDatabaseId ) );
    }

    @Override
    public void startedDownloadingSnapshot( NamedDatabaseId namedDatabaseId )
    {
        user.info( "Started downloading snapshot for database '%s' ...", namedDatabaseId.name() );
    }

    @Override
    public void downloadSnapshotComplete( NamedDatabaseId namedDatabaseId )
    {
        user.info( "Download of snapshot for database '%s' complete.", namedDatabaseId.name() );
    }

    @Override
    public void retryPublishRaftId( NamedDatabaseId namedDatabaseId, RaftGroupId raftGroupId )
    {
        publishRaftIdLog.info( "Failed to publish %s for database %s but will retry.", raftGroupId, namedDatabaseId );
    }

    @Override
    public void logSaveSystemDatabase()
    {
        debug.info( "Temporarily moving system database to force store copy" );
    }

    @Override
    public void bootstrapAttemptUsingDiscovery( NamedDatabaseId namedDatabaseId )
    {
        discoveryServiceAttemptLog.info( "Trying bootstrap of %s using discovery service method", namedDatabaseId );
    }

    @Override
    public void bootstrapAttempt( Set<ServerId> initialServers, StoreId storeId )
    {
        initialServersAttemptLog.info( "Trying bootstrap using initial servers %s and %s", initialServers, storeId );
    }
}
