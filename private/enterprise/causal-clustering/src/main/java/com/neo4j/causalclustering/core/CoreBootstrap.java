/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.identity.BoundState;
import com.neo4j.causalclustering.identity.RaftBinder;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.DatabaseStartAborter;

import java.util.Optional;

import org.neo4j.kernel.database.Database;
import org.neo4j.scheduler.JobHandle;

class CoreBootstrap
{
    private final Database kernelDatabase;
    private final RaftBinder raftBinder;

    private final LifecycleMessageHandler<?> raftMessageHandler;
    private final CoreSnapshotService snapshotService;
    private final CoreDownloaderService downloadService;
    private final ClusterInternalDbmsOperator clusterInternalOperator;
    private final DatabaseStartAborter databaseStartAborter;

    CoreBootstrap( Database kernelDatabase, RaftBinder raftBinder, LifecycleMessageHandler<?> raftMessageHandler, CoreSnapshotService snapshotService,
            CoreDownloaderService downloadService, ClusterInternalDbmsOperator clusterInternalOperator, DatabaseStartAborter databaseStartAborter )
    {
        this.kernelDatabase = kernelDatabase;
        this.raftBinder = raftBinder;
        this.raftMessageHandler = raftMessageHandler;
        this.snapshotService = snapshotService;
        this.downloadService = downloadService;
        this.clusterInternalOperator = clusterInternalOperator;
        this.databaseStartAborter = databaseStartAborter;
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
            }
            catch ( Exception e )
            {
                raftMessageHandler.stop();
                throw e;
            }
        }
    }

    private void awaitState() throws Exception
    {
        snapshotService.awaitState( databaseStartAborter );
        Optional<JobHandle> downloadJob = downloadService.downloadJob();
        if ( downloadJob.isPresent() )
        {
            downloadJob.get().waitTermination();
        }
    }
}
