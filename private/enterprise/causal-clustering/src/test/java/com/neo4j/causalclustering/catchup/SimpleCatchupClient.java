/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors;
import com.neo4j.causalclustering.catchup.storecopy.StreamToDiskProvider;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreIdProvider;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

/**
 * For use in ITs, for mocking purposes see {@link MockCatchupClient}
 */
class SimpleCatchupClient implements AutoCloseable
{
    private final GraphDatabaseAPI graphDb;
    private final FileSystemAbstraction fsa;
    private final CatchupClientFactory catchUpClientFactory;
    private final TestCatchupServer catchupServer;
    private final NamedDatabaseId namedDatabaseId;

    private final SocketAddress from;
    private final StoreId correctStoreId;
    private final StreamToDiskProvider streamToDiskProvider;
    private final PageCache clientPageCache;
    private final JobScheduler jobScheduler;
    private final Log log;
    private final LogProvider logProvider;

    SimpleCatchupClient( GraphDatabaseAPI graphDb, NamedDatabaseId namedDatabaseId, FileSystemAbstraction fileSystemAbstraction,
            CatchupClientFactory catchUpClientFactory, TestCatchupServer catchupServer, Path temporaryDirectory, LogProvider logProvider )
    {
        this.graphDb = graphDb;
        this.fsa = fileSystemAbstraction;
        this.catchUpClientFactory = catchUpClientFactory;
        this.catchupServer = catchupServer;
        this.namedDatabaseId = namedDatabaseId;

        from = getCatchupServerAddress();
        correctStoreId = graphDb.getDependencyResolver().resolveDependency( StoreIdProvider.class ).getStoreId();
        jobScheduler = new ThreadPoolJobScheduler();
        clientPageCache = createPageCache();
        streamToDiskProvider = new StreamToDiskProvider( temporaryDirectory, fsa, new Monitors() );
        log = logProvider.getLog( SimpleCatchupClient.class );
        this.logProvider = logProvider;
    }

    public PrepareStoreCopyResponse requestListOfFilesFromServer() throws Exception
    {
        return requestListOfFilesFromServer( correctStoreId, namedDatabaseId );
    }

    public PrepareStoreCopyResponse requestListOfFilesFromServer( StoreId expectedStoreId, NamedDatabaseId expectedNamedDatabaseId ) throws Exception
    {
        CatchupResponseAdaptor<PrepareStoreCopyResponse> responseHandler =
                StoreCopyResponseAdaptors.prepareStoreCopyAdaptor( streamToDiskProvider, logProvider.getLog( SimpleCatchupClient.class ) );
        return catchUpClientFactory.getClient( from, log )
                .v3( c -> c.prepareStoreCopy( expectedStoreId, expectedNamedDatabaseId ) )
                .v4( c -> c.prepareStoreCopy( expectedStoreId, expectedNamedDatabaseId ) )
                .withResponseHandler( responseHandler )
                .request();
    }

    public StoreCopyFinishedResponse requestIndividualFile( Path file ) throws Exception
    {
        return requestIndividualFile( file, correctStoreId, namedDatabaseId );
    }

    public StoreCopyFinishedResponse requestIndividualFile( Path file, StoreId expectedStoreId, NamedDatabaseId expectedNamedDatabaseId ) throws Exception
    {
        long lastTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        CatchupResponseAdaptor<StoreCopyFinishedResponse> responseHandler = StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, log );
        return catchUpClientFactory.getClient( from, log )
                .v3( c -> c.getStoreFile( expectedStoreId, file, lastTransactionId, expectedNamedDatabaseId ) )
                .v4( c -> c.getStoreFile( expectedStoreId, file, lastTransactionId, expectedNamedDatabaseId ) )
                .withResponseHandler( responseHandler )
                .request();
    }

    @Override
    public void close() throws Exception
    {
        IOUtils.closeAll( clientPageCache, jobScheduler );
    }

    private static CheckPointer getCheckPointer( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
    }

    private SocketAddress getCatchupServerAddress()
    {
        return new SocketAddress( "localhost", catchupServer.address().getPort() );
    }

    private PageCache createPageCache()
    {
        return StandalonePageCacheFactory.createPageCache( fsa, jobScheduler, PageCacheTracer.NULL );
    }

    public InfoResponse requestReconcilerInfo( NamedDatabaseId namedDatabaseId ) throws Exception
    {
        return catchUpClientFactory.getClient( from, log )
                .v3( c -> c.getReconciledInfo( namedDatabaseId ) )
                .v4( c -> c.getReconciledInfo( namedDatabaseId ) )
                .withResponseHandler( new CatchupResponseAdaptor<>()
                {
                    @Override
                    public void onInfo( CompletableFuture<InfoResponse> signal, InfoResponse response )
                    {
                        signal.complete( response );
                    }
                } ).request();
    }
}
