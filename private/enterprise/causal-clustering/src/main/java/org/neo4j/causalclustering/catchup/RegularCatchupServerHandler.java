/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import io.netty.channel.ChannelHandler;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyFilesProvider;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyRequestHandler;
import org.neo4j.causalclustering.catchup.storecopy.StoreFileStreamingProtocol;
import org.neo4j.causalclustering.catchup.tx.TxPullRequestHandler;
import org.neo4j.causalclustering.core.state.CoreSnapshotService;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequestHandler;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;

public class RegularCatchupServerHandler implements CatchupServerHandler
{

    private final Monitors monitors;
    private final LogProvider logProvider;
    private final Supplier<StoreId> storeIdSupplier;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;
    private final BooleanSupplier dataSourceAvailabilitySupplier;
    private final FileSystemAbstraction fs;
    private final CoreSnapshotService snapshotService;
    private final CheckPointerService checkPointerService;

    public RegularCatchupServerHandler( Monitors monitors, LogProvider logProvider, Supplier<StoreId> storeIdSupplier,
            Supplier<NeoStoreDataSource> dataSourceSupplier, BooleanSupplier dataSourceAvailabilitySupplier, FileSystemAbstraction fs,
            CoreSnapshotService snapshotService, CheckPointerService checkPointerService )
    {
        this.monitors = monitors;
        this.logProvider = logProvider;
        this.storeIdSupplier = storeIdSupplier;
        this.dataSourceSupplier = dataSourceSupplier;
        this.dataSourceAvailabilitySupplier = dataSourceAvailabilitySupplier;
        this.fs = fs;
        this.snapshotService = snapshotService;
        this.checkPointerService = checkPointerService;
    }

    @Override
    public ChannelHandler txPullRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new TxPullRequestHandler( catchupServerProtocol, storeIdSupplier, dataSourceAvailabilitySupplier, dataSourceSupplier,
                monitors, logProvider );
    }

    @Override
    public ChannelHandler getStoreIdRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new GetStoreIdRequestHandler( catchupServerProtocol, storeIdSupplier );
    }

    @Override
    public ChannelHandler storeListingRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new PrepareStoreCopyRequestHandler( catchupServerProtocol, dataSourceSupplier, new PrepareStoreCopyFilesProvider( fs ) );
    }

    @Override
    public ChannelHandler getStoreFileRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetStoreFileRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerService,
                new StoreFileStreamingProtocol(), fs, logProvider );
    }

    @Override
    public ChannelHandler getIndexSnapshotRequestHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return new StoreCopyRequestHandler.GetIndexSnapshotRequestHandler( catchupServerProtocol, dataSourceSupplier, checkPointerService,
                new StoreFileStreamingProtocol(), fs, logProvider );
    }

    @Override
    public Optional<ChannelHandler> snapshotHandler( CatchupServerProtocol catchupServerProtocol )
    {
        return Optional.ofNullable( (snapshotService != null) ? new CoreSnapshotRequestHandler( catchupServerProtocol, snapshotService ) : null );
    }
}
