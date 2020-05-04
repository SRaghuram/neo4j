/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreFileStream;
import com.neo4j.causalclustering.catchup.storecopy.StoreFileStreamProvider;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.common.ClusterMonitors;
import io.netty.buffer.ByteBuf;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.internal.helpers.ConstantTimeTimeoutStrategy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.catchup.storecopy.TerminationCondition.CONTINUE_INDEFINITELY;

class StoreCopyClientWrapper
{
    private final File ignoredDestDir = new File( "." );
    private final StoreCopyClient storeCopyClient;
    private final CatchupAddressProvider catchupAddressProvider;

    private static final StoreFileStream NOOP_STREAM = new StoreFileStream()
    {
        @Override
        public void write( ByteBuf byteBuf ) throws IOException
        {
            // no-op
        }

        @Override
        public void close() throws Exception
        {
            // no-op
        }
    };

    private static final StoreFileStreamProvider NOOP_STREAM_PROVIDER = new StoreFileStreamProvider()
    {
        @Override
        public StoreFileStream acquire( String destination, int requiredAlignment ) throws IOException
        {
            return NOOP_STREAM;
        }
    };

    StoreCopyClientWrapper( GlobalModule module, CatchupClientFactory catchupClientFactory, NamedDatabaseId databaseId, LogProvider logProvider,
            SocketAddress socketAddress )
    {

        var monitors = ClusterMonitors.create( module.getGlobalMonitors(), module.getGlobalDependencies() );
        var backupStrategy = new ConstantTimeTimeoutStrategy( 5, TimeUnit.SECONDS );
        this.storeCopyClient = new StoreCopyClient( catchupClientFactory, databaseId, () -> monitors, logProvider, backupStrategy );
        this.catchupAddressProvider = new CatchupAddressProvider.SingleAddressProvider( socketAddress );
    }

    void storeCopy() throws CatchupAddressResolutionException, StoreCopyFailedException, StoreIdDownloadFailedException
    {
        var storeId = storeCopyClient.fetchStoreId( catchupAddressProvider.primary( null ) );
        storeCopyClient.copyStoreFiles( catchupAddressProvider, storeId, NOOP_STREAM_PROVIDER, () -> CONTINUE_INDEFINITELY, ignoredDestDir );
    }
}
