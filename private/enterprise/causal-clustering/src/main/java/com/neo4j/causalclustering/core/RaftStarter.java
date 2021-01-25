/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.BootstrapSaver;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;

import java.time.Duration;
import java.util.Optional;

import org.neo4j.io.state.SimpleStorage;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.scheduler.JobHandle;

import static java.lang.String.format;

class RaftStarter extends LifecycleAdapter
{
    private final Database kernelDatabase;
    private final RaftBinder raftBinder;

    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final ClusterInternalDbmsOperator clusterInternalOperator;
    private final DatabaseStartAborter databaseStartAborter;
    private final SimpleStorage<RaftGroupId> raftIdGroupStorage;
    private final BootstrapSaver bootstrapSaver;
    private final TempBootstrapDir tempBootstrapDir;
    private final Lifecycle raftComponents;

    RaftStarter( Database kernelDatabase, RaftBinder raftBinder, LifecycleMessageHandler<?> raftMessageHandler, CoreSnapshotService snapshotService,
            CoreDownloaderService downloadService, ClusterInternalDbmsOperator clusterInternalOperator, DatabaseStartAborter databaseStartAborter,
            SimpleStorage<RaftGroupId> raftIdGroupStorage, BootstrapSaver bootstrapSaver, TempBootstrapDir tempBootstrapDir, Lifecycle raftComponents )
    {
        this.kernelDatabase = kernelDatabase;
        this.raftBinder = raftBinder;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
        this.clusterInternalOperator = clusterInternalOperator;
        this.databaseStartAborter = databaseStartAborter;
        this.raftIdGroupStorage = raftIdGroupStorage;
        this.bootstrapSaver = bootstrapSaver;
        this.tempBootstrapDir = tempBootstrapDir;
        this.raftComponents = raftComponents;
    }

    @Override
    public void start() throws Exception
    {
        var bootstrapHandle = clusterInternalOperator.bootstrap( kernelDatabase.getNamedDatabaseId() );
        try
        {
            bindAndStartMessageHandler();
        }
        finally
        {
            bootstrapHandle.release();
            databaseStartAborter.started( kernelDatabase.getNamedDatabaseId() );
        }
    }

    @Override
    public void stop() throws Exception
    {
        raftComponents.shutdown();
    }

    private void bindAndStartMessageHandler() throws Exception
    {
        bootstrapSaver.restore( kernelDatabase.getDatabaseLayout() );
        // Previous versions could leave "temp-bootstrap" around, so this deletion is a cleanup of that.
        tempBootstrapDir.delete();

        BoundState boundState = raftBinder.bindToRaft( databaseStartAborter );
        raftComponents.start();

        if ( boundState.snapshot().isPresent() )
        {
            // this means that we bootstrapped the cluster
            snapshotService.installSnapshot( boundState.snapshot().get() );
            raftMessageHandler.start( boundState.raftGroupId() );
        }
        else
        {
            raftMessageHandler.start( boundState.raftGroupId() );
            try
            {
                awaitState();
                bootstrapSaver.clean( kernelDatabase.getDatabaseLayout() );
            }
            catch ( Exception e )
            {
                downloadService.stop(); // The handler might be blocked doing a download, so we need to stop it.
                raftMessageHandler.stop();
                throw e;
            }
        }
        if ( raftIdGroupStorage.exists() )
        {
            var raftGroupIdStore = raftIdGroupStorage.readState();
            if ( !raftGroupIdStore.equals( boundState.raftGroupId() ) )
            {
                throw new IllegalStateException(
                        format( "Existing raft group id '%s' is different from bound state '%s'.", raftGroupIdStore.uuid(), boundState.raftGroupId().uuid() ) );
            }
        }
        else
        {
            raftIdGroupStorage.writeState( boundState.raftGroupId() );
        }
    }

    private void awaitState() throws Exception
    {
        var waitTime = Duration.ofSeconds( 1 );
        snapshotService.awaitState( databaseStartAborter, waitTime );
        Optional<JobHandle<?>> downloadJob = downloadService.downloadJob();
        if ( downloadJob.isPresent() )
        {
            downloadJob.get().waitTermination();
        }
    }
}
