/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.remote;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.TransactionLogCatchUpFactory;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients;
import com.neo4j.causalclustering.catchup.storecopy.DatabaseIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import com.neo4j.causalclustering.catchup.storecopy.RemoteStore;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClient;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyClientMonitor;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFailedException;
import com.neo4j.causalclustering.catchup.storecopy.StoreIdDownloadFailedException;
import com.neo4j.causalclustering.catchup.tx.TxPullClient;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponse;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.IncludeMetadata;
import com.neo4j.configuration.CausalClusteringInternalSettings;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.time.SystemNanoClock;

import static org.neo4j.internal.helpers.DefaultTimeoutStrategy.exponential;
import static org.neo4j.monitoring.PanicEventGenerator.NO_OP;

public class BackupClient
{
    private final Log log;
    private final CatchupClientFactory catchupClientFactory;
    private final BiFunction<StoreCopyClientMonitor,NamedDatabaseId,RemoteStore> remoteStoreFactory;

    public BackupClient( LogProvider logProvider, CatchupClientFactory catchupClientFactory, FileSystemAbstraction fsa, PageCache pageCache,
                         PageCacheTracer pageCacheTracer, Monitors monitors,
                         Config config, StorageEngineFactory storageEngineFactory, SystemNanoClock clock, JobScheduler jobScheduler )
    {
        this.log = logProvider.getLog( getClass() );
        this.catchupClientFactory = catchupClientFactory;
        var backoffStrategy = exponential( 1, config.get( CausalClusteringInternalSettings.store_copy_backoff_max_wait ).toMillis(),
                                           TimeUnit.MILLISECONDS );
        this.remoteStoreFactory = ( storeCopyClientMonitor, namedDatabaseId ) ->
        {
            var childMonitor = new Monitors( monitors );
            childMonitor.addMonitorListener( storeCopyClientMonitor );
            var txPullClient = new TxPullClient( catchupClientFactory, namedDatabaseId, () -> childMonitor, logProvider );
            final var storeCopyExecutor = jobScheduler.executor( Group.STORE_COPY_CLIENT );
            var storeCopyClient =
                    new StoreCopyClient( catchupClientFactory, namedDatabaseId, () -> childMonitor, logProvider, storeCopyExecutor, backoffStrategy, clock );
            var transactionLogFactory = new TransactionLogCatchUpFactory();
            var databaseHealth = new DatabaseHealth( NO_OP, logProvider.getLog( BackupClient.class ) );
            return new RemoteStore( logProvider, fsa, pageCache,
                                    storeCopyClient,
                                    txPullClient, transactionLogFactory, config, childMonitor,
                                    storageEngineFactory, namedDatabaseId, pageCacheTracer,
                                    EmptyMemoryTracker.INSTANCE, clock, null, databaseHealth );
        };
    }

    public Set<NamedDatabaseId> getAllDatabaseNames( SocketAddress address ) throws Exception
    {
        return catchupClientFactory.getClient( address, log )
                                   .v3( VersionedCatchupClients.CatchupClientV3::getAllDatabaseIds )
                                   .v4( VersionedCatchupClients.CatchupClientV4::getAllDatabaseIds )
                                   .v5( VersionedCatchupClients.CatchupClientV5::getAllDatabaseIds )
                                   .withResponseHandler( new CatchupResponseAdaptor<>()
                                   {
                                       @Override
                                       public void onGetAllDatabaseIdsResponse( CompletableFuture<GetAllDatabaseIdsResponse> signal,
                                                                                GetAllDatabaseIdsResponse response )
                                       {
                                           signal.complete( response );
                                       }
                                   } ).request().databaseIds();
    }

    public List<String> downloadMetaData( SocketAddress address, String databaseName, IncludeMetadata includeMetadata )
    {
        List<String> metaData = List.of();
        log.info( "Requesting metadata for database '%s'.", databaseName );
        try
        {
            metaData = catchupClientFactory.getClient( address, log )
                                           .v3( client -> client.getMetadata( databaseName, includeMetadata.name() ) )
                                           .v4( client -> client.getMetadata( databaseName, includeMetadata.name() ) )
                                           .v5( client -> client.getMetadata( databaseName, includeMetadata.name() ) )

                                           .withResponseHandler( new CatchupResponseAdaptor<>()
                                           {
                                               @Override
                                               public void onGetMetadataResponse( CompletableFuture<GetMetadataResponse> signal, GetMetadataResponse response )
                                               {
                                                   signal.complete( response );
                                               }
                                           } )
                                           .request()
                                           .commands();
        }
        catch ( Exception ex )
        {
            log.warn( "Error in execution of get metadata, it will be ignored", ex );
        }
        return metaData;
    }

    public RemoteInfo prepareToBackup( SocketAddress address, String databaseName ) throws Exception
    {

        log.info( "Begin preparing for backup for database '%s' from %s.", databaseName, address );
        var namedDatabaseId = getDatabaseId( address, databaseName );
        var remoteStoreId = getStoreId( address, databaseName, namedDatabaseId );
        return new RemoteInfo( address, remoteStoreId, namedDatabaseId );
    }

    private StoreId getStoreId( SocketAddress address, String databaseName, NamedDatabaseId namedDatabaseId ) throws StoreIdDownloadFailedException
    {
        log.info( "Requesting store id for database '%s'.", databaseName );
        try
        {
            StoreId remoteStoreId = catchupClientFactory.getClient( address, log )
                                                        .v3( client -> client.getStoreId( namedDatabaseId ) )
                                                        .v4( client -> client.getStoreId( namedDatabaseId ) )
                                                        .v5( client -> client.getStoreId( namedDatabaseId ) )
                                                        .withResponseHandler( new CatchupResponseAdaptor<>()
                                                        {
                                                            @Override
                                                            public void onGetStoreIdResponse( CompletableFuture<StoreId> signal, GetStoreIdResponse response )
                                                            {
                                                                signal.complete( response.storeId() );
                                                            }
                                                        } ).request();
            log.info( "Received database id for database '%s'.", namedDatabaseId );
            return remoteStoreId;
        }
        catch ( Exception ex )
        {
            var msg = String.format( "Failed to download store id for database '%s' from '%s'", databaseName, address );
            throw new StoreIdDownloadFailedException( msg, ex );
        }
    }

    private NamedDatabaseId getDatabaseId( SocketAddress address, String databaseName ) throws Exception
    {
        log.info( "Requesting database id for %s.", databaseName );
        try
        {
            var namedDatabaseId = catchupClientFactory.getClient( address, log )
                                                      .v3( client -> client.getDatabaseId( databaseName ) )
                                                      .v4( client -> client.getDatabaseId( databaseName ) )
                                                      .v5( client -> client.getDatabaseId( databaseName ) )
                                                      .withResponseHandler( new CatchupResponseAdaptor<>()
                                                      {
                                                          @Override
                                                          public void onGetDatabaseIdResponse( CompletableFuture<NamedDatabaseId> signal,
                                                                                               GetDatabaseIdResponse response )
                                                          {
                                                              signal.complete( DatabaseIdFactory.from( databaseName, response.databaseId().uuid() ) );
                                                          }
                                                      } ).request();
            log.info( "Received database id for database '%s'.", namedDatabaseId );
            return namedDatabaseId;
        }
        catch ( Exception ex )
        {
            var msg = String.format( "Failed to download database id for database '%s' from '%s'", databaseName, address );
            throw new DatabaseIdDownloadFailedException( msg, ex );
        }
    }

    public void updateStore( RemoteInfo info, DatabaseLayout databaseLayout, StoreCopyClientMonitor backupOutputMonitor )
            throws StoreCopyFailedException, IOException
    {
        var catchupAddressProvider = new CatchupAddressProvider.SingleAddressProvider( info.address() );
        var remoteStore = remoteStoreFactory.apply( backupOutputMonitor, info.namedDatabaseId() );
        remoteStore.tryCatchingUp( catchupAddressProvider, info.storeId(), databaseLayout, true );
    }

    public void fullStoreCopy( RemoteInfo info, DatabaseLayout databaseLayout, StoreCopyClientMonitor backupOutputMonitor )
            throws StoreCopyFailedException
    {
        var addressProvider = new CatchupAddressProvider.SingleAddressProvider( info.address() );
        var remoteStore = remoteStoreFactory.apply( backupOutputMonitor, info.namedDatabaseId() );
        remoteStore.copy( addressProvider, info.storeId(), databaseLayout );
    }
}
