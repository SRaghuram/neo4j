/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.micro.benchmarks.cluster.LocalNetworkPlatform;
import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.RequestMessageType;
import com.neo4j.causalclustering.catchup.storecopy.FileChunk;
import com.neo4j.causalclustering.catchup.storecopy.FileHeader;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequest;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponse;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.StoreId;

class BareClient implements CatchupResponseHandler
{
    private final Log log;
    private final LocalNetworkPlatform platform;

    private Consumer<Exception> errorCallback;
    private Runnable callback;

    BareClient( LogProvider logProvider, LocalNetworkPlatform platform )
    {
        this.log = logProvider.getLog( getClass() );
        this.platform = platform;
    }

    private DatabaseId databaseId;
    private StoreId storeId;
    private long lastCheckPointedTxId;
    private Iterator<Path> files;

    void registerErrorCallback( Consumer<Exception> errorCallback )
    {
        this.errorCallback = errorCallback;
    }

    @Override
    public void onGetDatabaseIdResponse( GetDatabaseIdResponse response )
    {
        databaseId = response.databaseId();
        log.info( "Received GetDatabaseIdResponse with %s", databaseId );
        callback();
    }

    @Override
    public void onGetStoreIdResponse( GetStoreIdResponse response )
    {
        storeId = response.storeId();
        log.info( "Received GetStoreIdResponse with %s", storeId );
        callback();
    }

    @Override
    public void onStoreListingResponse( PrepareStoreCopyResponse response )
    {
        var files = response.getPaths();
        this.files = List.of( files ).iterator();
        lastCheckPointedTxId = response.lastCheckPointedTransactionId();
        log.info( "Received PrepareStoreCopyResponse with %s,files=%d, %d", response.status(), files.length, lastCheckPointedTxId );
        callback();
    }

    @Override
    public void onFileStreamingComplete( StoreCopyFinishedResponse response )
    {
        lastCheckPointedTxId = response.lastCheckpointedTx();
        log.info( "Received StoreCopyFinishedResponse with %s, %d", response.status(), lastCheckPointedTxId );
        callback();
    }

    @Override
    public void onTxStreamFinishedResponse( TxStreamFinishedResponse response )
    {
        log.info( "Received TxStreamFinishedResponse with %s, %d", response.status(), response.lastTxId() );
        callback();
    }

    @Override
    public void onCoreSnapshot( CoreSnapshot response )
    {
        log.info( "Received CoreSnapshot with %d, %d", response.prevTerm(), response.prevIndex() );
        callback();
    }

    @Override
    public void onCatchupErrorResponse( CatchupErrorResponse response )
    {
        log.error( "This onCatchupErrorResponse should have never been called with %s, %s", response.status(), response.message() );
        callbackError( new IllegalArgumentException( String.format( "onCatchupErrorResponse: %s, %s", response.status(), response.message() ) ) );
    }

    @Override
    public void onGetAllDatabaseIdsResponse( GetAllDatabaseIdsResponse response )
    {
        log.error( "This onGetAllDatabaseIdsResponse should have never been called with %s", response.databaseIds() );
        callbackError( new IllegalArgumentException( String.format( "onGetAllDatabaseIdsResponse: %s", response.databaseIds() ) ) );
    }

    @Override
    public void onInfo( InfoResponse response )
    {
        log.error( "This onInfo should have never been called with %s", response );
        callbackError( new IllegalArgumentException( String.format( "onInfo: %s", response ) ) );
    }

    @Override
    public void onGetMetadataResponse( GetMetadataResponse response )
    {
        log.error( "This onGetMetadataResponse should have never been called with %s", response.commands() );
        callbackError( new IllegalArgumentException( String.format( "onGetMetadataResponse: %s", response.commands() ) ) );
    }

    @Override
    public void onClose()
    {
        log.error( "This onClose should have never been called" );
        callbackError( new IllegalArgumentException( "client onClose" ) );
    }

    @Override
    public void onFileHeader( FileHeader fileHeader )
    {
        log.info( "Received FileHeader with %s", fileHeader.fileName() );
    }

    @Override
    public boolean onFileContent( FileChunk fileChunk )
    {
        return fileChunk.isLast();
    }

    @Override
    public void onTxPullResponse( ReceivedTxPullResponse tx )
    {
        log.info( "Received TxPullResponse with %s,%s", tx.storeId(), tx.tx() );
    }

    void getDatabaseId( Runnable callback, String databaseName )
    {
        request( callback, RequestMessageType.DATABASE_ID, new GetDatabaseIdRequest( databaseName ) );
    }

    void getStoreId( Runnable callback )
    {
        request( callback, RequestMessageType.STORE_ID, new GetStoreIdRequest( databaseId ) );
    }

    void prepareStoreCopy( Runnable callback )
    {
        request( callback, RequestMessageType.PREPARE_STORE_COPY, new PrepareStoreCopyRequest( storeId, databaseId ) );
    }

    boolean hasNextFile()
    {
        return files.hasNext();
    }

    void getNextFile( Runnable callback )
    {
        request( callback, RequestMessageType.STORE_FILE, new GetStoreFileRequest( storeId, files.next(), lastCheckPointedTxId, databaseId ) );
    }

    void pullTransactions( Runnable callback )
    {
        request( callback, RequestMessageType.TX_PULL_REQUEST, new TxPullRequest( lastCheckPointedTxId, storeId, databaseId ) );
    }

    private void request( Runnable callback, Object... parts )
    {
        if ( this.errorCallback == null )
        {
            throw new IllegalStateException( "Error Callback not defined" );
        }
        if ( this.callback != null )
        {
            callbackError( new InterruptedException() );
            return;
        }
        this.callback = callback;
        try
        {
            var channel = platform.channel();
            Stream.of( parts ).forEach( channel::write );
            channel.flush();
        }
        catch ( Exception e )
        {
            callbackError( e );
        }
    }

    private void callback()
    {
        if ( this.callback == null )
        {
            callbackError( new InterruptedException() );
            return;
        }
        try
        {
            var callback = this.callback;
            this.callback = null;
            callback.run();
        }
        catch ( Exception e )
        {
            callbackError( e );
        }
    }

    private void callbackError( Exception e )
    {
        try
        {
            errorCallback.accept( e );
            callback = null;
        }
        catch ( Throwable t )
        {
            log.error( "Callback unsuccessful", t );
        }
    }
}
