/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.BootstrapSaver;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.identity.RaftId;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;

import java.time.Duration;
import java.util.Optional;

import org.neo4j.kernel.database.Database;
import org.neo4j.scheduler.JobHandle;

import static java.lang.String.format;

class CoreBootstrap
{
    private final Database kernelDatabase;
    private final RaftBinder raftBinder;

    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final ClusterInternalDbmsOperator clusterInternalOperator;
    private final DatabaseStartAborter databaseStartAborter;
    private final SimpleStorage<RaftId> raftIdStorage;
    private final BootstrapSaver bootstrapSaver;

    CoreBootstrap( Database kernelDatabase, RaftBinder raftBinder, LifecycleMessageHandler<?> raftMessageHandler, CoreSnapshotService snapshotService,
            CoreDownloaderService downloadService, ClusterInternalDbmsOperator clusterInternalOperator, DatabaseStartAborter databaseStartAborter,
            SimpleStorage<RaftId> raftIdStorage, BootstrapSaver bootstrapSaver )
    {
        this.kernelDatabase = kernelDatabase;
        this.raftBinder = raftBinder;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
        this.clusterInternalOperator = clusterInternalOperator;
        this.databaseStartAborter = databaseStartAborter;
        this.raftIdStorage = raftIdStorage;
        this.bootstrapSaver = bootstrapSaver;
    }

    public void perform() throws Exception
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

    private void bindAndStartMessageHandler() throws Exception
    {
        bootstrapSaver.restore( kernelDatabase.getDatabaseLayout() );
        BoundState boundState = raftBinder.bindToRaft( databaseStartAborter );

        if ( boundState.snapshot().isPresent() )
        {
            // this means that we bootstrapped the cluster
            snapshotService.installSnapshot( boundState.snapshot().get() );
            raftMessageHandler.start( boundState.raftId() );
        }
        else
        {
            raftMessageHandler.start( boundState.raftId() );
            try
            {
                awaitState();
                bootstrapSaver.clean( kernelDatabase.getDatabaseLayout() );
            }
            catch ( Exception e )
            {
                raftMessageHandler.stop();
                throw e;
            }
        }
        if ( raftIdStorage.exists() )
        {
            var raftIdStore = raftIdStorage.readState();
            if ( !raftIdStore.equals( boundState.raftId() ) )
            {
                throw new IllegalStateException(
                        format( "Exiting raft id '%s' is different from bound state '%s'.", raftIdStore.uuid(), boundState.raftId().uuid() ) );
            }
        }
        else
        {
            raftIdStorage.writeState( boundState.raftId() );
        }
    }

    private void awaitState() throws Exception
    {
        var waitTime = Duration.ofSeconds( 1 );
        snapshotService.awaitState( databaseStartAborter, waitTime );
        Optional<JobHandle> downloadJob = downloadService.downloadJob();
        if ( downloadJob.isPresent() )
        {
            downloadJob.get().waitTermination();
        }
    }
}
