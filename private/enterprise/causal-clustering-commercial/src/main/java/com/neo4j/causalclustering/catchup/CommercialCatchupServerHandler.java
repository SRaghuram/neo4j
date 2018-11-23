/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandler;

import java.util.Optional;

import org.neo4j.causalclustering.catchup.CatchupServerHandler;
import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyRequestHandler.GetIndexSnapshotRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyRequestHandler.GetStoreFileRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import org.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.common.LocalDatabase;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.LogProvider;

/**
 * The commercial catchup server multiplexes requests for multiple databases.
 */
public class CommercialCatchupServerHandler implements CatchupServerHandler
{
    private final DatabaseService databaseService;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fs;
    private final CoreSnapshotService snapshotService;

    public CommercialCatchupServerHandler( DatabaseService databaseService, LogProvider logProvider, FileSystemAbstraction fs )
    {
        this( databaseService, logProvider, fs, null );
    }

    public CommercialCatchupServerHandler( DatabaseService databaseService, LogProvider logProvider, FileSystemAbstraction fs,
            CoreSnapshotService snapshotService )
    {
        this.databaseService = databaseService;
        this.logProvider = logProvider;
        this.fs = fs;
        this.snapshotService = snapshotService;
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, db -> buildTxPullRequestHandler( db, protocol ), TxPullRequest.class, databaseService );
    }

    private TxPullRequestHandler buildTxPullRequestHandler( LocalDatabase localDatabase, CatchupServerProtocol protocol )
    {
        //TODO: move these to a factory method on the Handlers, enforced by an interface?
        return new TxPullRequestHandler( protocol, localDatabase::storeId, databaseService::areAvailable,
                localDatabase::database, localDatabase.monitors(), logProvider );
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, db -> buildStoreIdRequestHandler( db, protocol ), GetStoreIdRequest.class, databaseService );
    }

    private GetStoreIdRequestHandler buildStoreIdRequestHandler( LocalDatabase localDatabase, CatchupServerProtocol protocol )
    {
        return new GetStoreIdRequestHandler( protocol, localDatabase::storeId );
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, db -> buildStoreListingRequestHandler( db, protocol ),
                PrepareStoreCopyRequest.class, databaseService );
    }

    private PrepareStoreCopyRequestHandler buildStoreListingRequestHandler( LocalDatabase localDatabase,
            CatchupServerProtocol protocol )
    {
        return new PrepareStoreCopyRequestHandler( protocol, localDatabase::database, new PrepareStoreCopyFilesProvider( fs ) );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, db -> buildStoreFileRequestHandler( db, protocol ),
                GetStoreFileRequest.class, databaseService );
    }

    private GetStoreFileRequestHandler buildStoreFileRequestHandler( LocalDatabase localDatabase,
            CatchupServerProtocol protocol )
    {
        return new GetStoreFileRequestHandler( protocol, localDatabase::database, localDatabase.checkPointerService(),
                new StoreFileStreamingProtocol(), fs, logProvider );
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, db -> buildIndexSnapshotRequestHandler( db, protocol ),
                GetIndexFilesRequest.class, databaseService );
    }

    private GetIndexSnapshotRequestHandler buildIndexSnapshotRequestHandler( LocalDatabase localDatabase,
            CatchupServerProtocol protocol )
    {
        return new GetIndexSnapshotRequestHandler( protocol, localDatabase::database, localDatabase.checkPointerService(),
                new StoreFileStreamingProtocol(), fs, logProvider );
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.ofNullable( snapshotService ).map( svc -> new CoreSnapshotRequestHandler( catchupServerProtocol, svc ) );
    }
}
