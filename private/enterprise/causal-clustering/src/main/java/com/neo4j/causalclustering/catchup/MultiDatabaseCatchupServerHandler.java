/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyRequestHandler.GetIndexSnapshotRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyRequestHandler.GetStoreFileRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import com.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import com.neo4j.causalclustering.core.state.CoreSnapshotService;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import io.netty.channel.ChannelHandler;

import java.util.Optional;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.LogProvider;

/**
 * The commercial catchup server multiplexes requests for multiple databases.
 */
public class MultiDatabaseCatchupServerHandler implements CatchupServerHandler
{
    private final DatabaseManager<?> databaseManager;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fs;
    private final CoreSnapshotService snapshotService;

    public MultiDatabaseCatchupServerHandler( DatabaseManager<?> databaseManager, LogProvider logProvider, FileSystemAbstraction fs )
    {
        this( databaseManager, logProvider, fs, null );
    }

    public MultiDatabaseCatchupServerHandler( DatabaseManager<?> databaseManager, LogProvider logProvider, FileSystemAbstraction fs,
            CoreSnapshotService snapshotService )
    {
        this.databaseManager = databaseManager;
        this.logProvider = logProvider;
        this.fs = fs;
        this.snapshotService = snapshotService;
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManager, db -> buildTxPullRequestHandler( db, protocol ),
                TxPullRequest.class, logProvider );
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManager, db -> buildStoreIdRequestHandler( db, protocol ),
                GetStoreIdRequest.class, logProvider );
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManager, db -> buildStoreListingRequestHandler( db, protocol ),
                PrepareStoreCopyRequest.class, logProvider );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManager, db -> buildStoreFileRequestHandler( db, protocol ),
                GetStoreFileRequest.class, logProvider );
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManager, db -> buildIndexSnapshotRequestHandler( db, protocol ),
                GetIndexFilesRequest.class, logProvider );
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.ofNullable( snapshotService ).map( svc -> new CoreSnapshotRequestHandler( catchupServerProtocol, svc ) );
    }

    private TxPullRequestHandler buildTxPullRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new TxPullRequestHandler( protocol, db, logProvider );
    }

    private GetStoreIdRequestHandler buildStoreIdRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreIdRequestHandler( protocol, db );
    }

    private PrepareStoreCopyRequestHandler buildStoreListingRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new PrepareStoreCopyRequestHandler( protocol, db, new PrepareStoreCopyFilesProvider( fs ), logProvider );
    }

    private GetStoreFileRequestHandler buildStoreFileRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreFileRequestHandler( protocol, db, new StoreFileStreamingProtocol(), fs, logProvider );
    }

    private GetIndexSnapshotRequestHandler buildIndexSnapshotRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetIndexSnapshotRequestHandler( protocol, db, new StoreFileStreamingProtocol(), fs, logProvider );
    }
}
