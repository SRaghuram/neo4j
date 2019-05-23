/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.GetStoreFileRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import com.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequestHandler;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import io.netty.channel.ChannelHandler;

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

    public MultiDatabaseCatchupServerHandler( DatabaseManager<?> databaseManager, LogProvider logProvider, FileSystemAbstraction fs )
    {
        this.databaseManager = databaseManager;
        this.logProvider = logProvider;
        this.fs = fs;
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
    public ChannelHandler snapshotHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManager, db -> buildCoreSnapshotRequestRequestHandler( db, protocol ),
                CoreSnapshotRequest.class, logProvider );
    }

    private static TxPullRequestHandler buildTxPullRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new TxPullRequestHandler( protocol, db );
    }

    private static GetStoreIdRequestHandler buildStoreIdRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreIdRequestHandler( protocol, db );
    }

    private PrepareStoreCopyRequestHandler buildStoreListingRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new PrepareStoreCopyRequestHandler( protocol, db, new PrepareStoreCopyFilesProvider( fs ) );
    }

    private GetStoreFileRequestHandler buildStoreFileRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreFileRequestHandler( protocol, db, new StoreFileStreamingProtocol(), fs );
    }

    private static CoreSnapshotRequestHandler buildCoreSnapshotRequestRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new CoreSnapshotRequestHandler( protocol, db );
    }
}
