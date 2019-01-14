/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.util.Optional;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.identity.BoundState;
import org.neo4j.causalclustering.identity.ClusterBinder;
import org.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import org.neo4j.kernel.lifecycle.SafeLifecycle;
import org.neo4j.kernel.recovery.RecoveryRequiredChecker;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobHandle;

public class CoreLife extends SafeLifecycle
{
    private final RaftMachine raftMachine;
    private final DatabaseService databaseService;
    private final ClusterBinder clusterBinder;

    private final CommandApplicationProcess applicationProcess;
    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final Log log;
    private final RecoveryRequiredChecker recoveryChecker;

    public CoreLife( RaftMachine raftMachine, DatabaseService databaseService, ClusterBinder clusterBinder,
            CommandApplicationProcess commandApplicationProcess, LifecycleMessageHandler<?> raftMessageHandler,
            CoreSnapshotService snapshotService, CoreDownloaderService downloadService, LogProvider logProvider,
            RecoveryRequiredChecker recoveryChecker )
    {
        this.raftMachine = raftMachine;
        this.databaseService = databaseService;
        this.clusterBinder = clusterBinder;
        this.applicationProcess = commandApplicationProcess;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
        this.log = logProvider.getLog( getClass() );
        this.recoveryChecker = recoveryChecker;
    }

    @Override
    public void init0() throws Throwable
    {
        databaseService.init();
    }

    @Override
    public void start0() throws Throwable
    {
        ensureRecovered();

        BoundState boundState = clusterBinder.bindToCluster();
        raftMessageHandler.start( boundState.clusterId() );

        boolean startedByDownloader = false;
        if ( boundState.snapshot().isPresent() )
        {
            // this means that we bootstrapped the cluster
            CoreSnapshot snapshot = boundState.snapshot().get();
            snapshotService.installSnapshot( snapshot );
        }
        else
        {
            snapshotService.awaitState();
            Optional<JobHandle> downloadJob = downloadService.downloadJob();
            if ( downloadJob.isPresent() )
            {
                downloadJob.get().waitTermination();
                startedByDownloader = true;
            }
        }

        if ( !startedByDownloader )
        {
            databaseService.start();
        }
        applicationProcess.start();
        raftMachine.postRecoveryActions();
    }

    private void ensureRecovered() throws Throwable
    {
        for ( LocalDatabase db : databaseService.registeredDatabases().values() )
        {
            /*  There is no reason to try to recover if there are no transaction logs and in fact it is
             *  also problematic for the initial transaction pull during the snapshot download because the
             *  kernel will create a transaction log with a header where previous index points to the same
             *  index as that written down into the metadata store. This is problematic because we have no
             *  guarantee that there are later transactions and we need at least one transaction in
             *  the log to figure out the Raft log index (see {@link RecoverConsensusLogIndex}). */

            if ( recoveryChecker.isRecoveryRequiredAt( db.databaseLayout() ) )
            {
                log.info( "Recovering " + db.databaseName() );
                databaseService.start();
                databaseService.stop();
            }
            else
            {
                log.info( "No recovery required for " + db.databaseName() );
            }
        }
    }

    @Override
    public void stop0() throws Throwable
    {
        raftMachine.stopTimers();
        raftMessageHandler.stop();
        applicationProcess.stop();
        databaseService.stop();
    }

    @Override
    public void shutdown0() throws Throwable
    {
        databaseService.shutdown();
    }
}
