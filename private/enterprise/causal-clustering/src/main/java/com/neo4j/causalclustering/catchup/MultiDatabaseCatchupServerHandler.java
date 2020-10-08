/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.GetStoreFileRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import com.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import com.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequestHandler;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequestHandler;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsRequestHandler;
import com.neo4j.causalclustering.catchup.v4.info.InfoRequestHandler;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataRequestHandler;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import io.netty.channel.ChannelHandler;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.LogProvider;

/**
 * The enterprise catchup server multiplexes requests for multiple databases.
 */
public class MultiDatabaseCatchupServerHandler implements CatchupServerHandler
{
    private final DatabaseManager<?> databaseManager;
    private final DependencyResolver dependencyResolver;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fs;
    private final DatabaseStateService databaseStateService;
    private final int maxChunkSize;

    public MultiDatabaseCatchupServerHandler( DatabaseManager<?> databaseManager, DatabaseStateService databaseStateService,
                                              FileSystemAbstraction fs, int maxChunkSize, LogProvider logProvider, DependencyResolver dependencyResolver )
    {
        this.databaseManager = databaseManager;
        this.databaseStateService = databaseStateService;
        this.maxChunkSize = maxChunkSize;
        this.logProvider = logProvider;
        this.fs = fs;
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public ChannelHandler getDatabaseIdRequestHandler( CatchupServerProtocol protocol )
    {
        return new GetDatabaseIdRequestHandler( databaseManager, protocol );
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

    @Override
    public ChannelHandler getAllDatabaseIds( CatchupServerProtocol protocol )
    {
        return buildGetAllDatabaseIdsRequestHandler( protocol, databaseManager );
    }

    @Override
    public ChannelHandler getInfo( CatchupServerProtocol protocol )
    {
        return buildInfoHandler( protocol, databaseManager, databaseStateService );
    }

    private static InfoRequestHandler buildInfoHandler( CatchupServerProtocol protocol, DatabaseManager<?> databaseManager,
                                                        DatabaseStateService databaseStateService )
    {
        return new InfoRequestHandler( protocol, databaseManager, databaseStateService );
    }

    @Override
    public ChannelHandler getMetadata( CatchupServerProtocol catchupServerProtocol )
    {
        return new GetMetadataRequestHandler( catchupServerProtocol, dependencyResolver, databaseManager );
    }

    private TxPullRequestHandler buildTxPullRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new TxPullRequestHandler( protocol, db );
    }

    private static GetStoreIdRequestHandler buildStoreIdRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreIdRequestHandler( protocol, db );
    }

    private PrepareStoreCopyRequestHandler buildStoreListingRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new PrepareStoreCopyRequestHandler( protocol, db, new PrepareStoreCopyFilesProvider( fs ), maxChunkSize );
    }

    private GetStoreFileRequestHandler buildStoreFileRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreFileRequestHandler( protocol, db, new StoreFileStreamingProtocol( maxChunkSize ), fs );
    }

    private static CoreSnapshotRequestHandler buildCoreSnapshotRequestRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new CoreSnapshotRequestHandler( protocol, db );
    }

    private static GetAllDatabaseIdsRequestHandler buildGetAllDatabaseIdsRequestHandler( CatchupServerProtocol protocol, DatabaseManager<?> databaseManager )
    {
        return new GetAllDatabaseIdsRequestHandler( protocol, databaseManager );
    }
}
