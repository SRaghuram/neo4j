/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandler;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

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
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.logging.LogProvider;

/**
 * The commercial catchup server multiplexes requests for multiple databases.
 */
public class CommercialCatchupServerHandler implements CatchupServerHandler
{
    private final Supplier<DatabaseManager> databaseManagerSupplier;
    private final LogProvider logProvider;
    private final FileSystemAbstraction fs;
    private final CoreSnapshotService snapshotService;

    public CommercialCatchupServerHandler( Supplier<DatabaseManager> databaseManagerSupplier, LogProvider logProvider, FileSystemAbstraction fs )
    {
        this( databaseManagerSupplier, logProvider, fs, null );
    }

    public CommercialCatchupServerHandler( Supplier<DatabaseManager> databaseManagerSupplier, LogProvider logProvider, FileSystemAbstraction fs,
            CoreSnapshotService snapshotService )
    {
        this.databaseManagerSupplier = databaseManagerSupplier;
        this.logProvider = logProvider;
        this.fs = fs;
        this.snapshotService = snapshotService;
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManagerSupplier, db -> buildTxPullRequestHandler( db, protocol ),
                TxPullRequest.class );
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManagerSupplier, db -> buildStoreIdRequestHandler( db, protocol ),
                GetStoreIdRequest.class );
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManagerSupplier, db -> buildStoreListingRequestHandler( db, protocol ),
                PrepareStoreCopyRequest.class );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManagerSupplier, db -> buildStoreFileRequestHandler( db, protocol ),
                GetStoreFileRequest.class );
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol protocol )
    {
        return new MultiplexingCatchupRequestHandler<>( protocol, databaseManagerSupplier, db -> buildIndexSnapshotRequestHandler( db, protocol ),
                GetIndexFilesRequest.class );
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.ofNullable( snapshotService ).map( svc -> new CoreSnapshotRequestHandler( catchupServerProtocol, svc ) );
    }

    private TxPullRequestHandler buildTxPullRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new TxPullRequestHandler( protocol, storeIdSupplier( db ), databaseAvailable( db ), () -> db, db.getMonitors(), logProvider );
    }

    private GetStoreIdRequestHandler buildStoreIdRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreIdRequestHandler( protocol, storeIdSupplier( db ) );
    }

    private PrepareStoreCopyRequestHandler buildStoreListingRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new PrepareStoreCopyRequestHandler( protocol, () -> db, new PrepareStoreCopyFilesProvider( fs ) );
    }

    private GetStoreFileRequestHandler buildStoreFileRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetStoreFileRequestHandler( protocol, () -> db, new StoreFileStreamingProtocol(), fs, logProvider );
    }

    private GetIndexSnapshotRequestHandler buildIndexSnapshotRequestHandler( Database db, CatchupServerProtocol protocol )
    {
        return new GetIndexSnapshotRequestHandler( protocol, () -> db, new StoreFileStreamingProtocol(), fs, logProvider );
    }

    private static Supplier<StoreId> storeIdSupplier( Database db )
    {
        return () -> new StoreId(
                db.getStoreId().getCreationTime(),
                db.getStoreId().getRandomId(),
                db.getStoreId().getUpgradeTime(),
                db.getStoreId().getUpgradeId() );
    }

    private static BooleanSupplier databaseAvailable( Database db )
    {
        return db.getDatabaseAvailabilityGuard()::isAvailable;
    }
}
