/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.messaging.StoreCopyRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static java.lang.String.format;
import static org.neo4j.io.fs.FileUtils.relativePath;

public abstract class StoreCopyRequestHandler<T extends StoreCopyRequest> extends SimpleChannelInboundHandler<T>
{
    private final CatchupServerProtocol protocol;
    private final Database db;
    private final StoreFileStreamingProtocol storeFileStreamingProtocol;

    private final FileSystemAbstraction fs;
    private final Log log;

    StoreCopyRequestHandler( CatchupServerProtocol protocol, Database db, StoreFileStreamingProtocol storeFileStreamingProtocol,
            FileSystemAbstraction fs, LogProvider logProvider, Class<T> clazz )
    {
        super( clazz );
        this.protocol = protocol;
        this.db = db;
        this.storeFileStreamingProtocol = storeFileStreamingProtocol;
        this.fs = fs;
        this.log = logProvider.getLog( StoreCopyRequestHandler.class );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext ctx, T request ) throws Exception
    {
        log.debug( "Handling request %s", request );
        StoreCopyFinishedResponse.Status responseStatus = StoreCopyFinishedResponse.Status.E_UNKNOWN;
        long lastCheckpointedTx = -1;
        try
        {
            CheckPointer checkPointer = db.getDependencyResolver().resolveDependency( CheckPointer.class );
            if ( !Objects.equals( request.expectedStoreId(), db.getStoreId() ) )
            {
                responseStatus = StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
            }
            else if ( checkPointer.lastCheckPointedTransactionId() < request.requiredTransactionId() )
            {
                responseStatus = StoreCopyFinishedResponse.Status.E_TOO_FAR_BEHIND;
                tryAsyncCheckpoint( db, checkPointer );
            }
            else
            {
                File databaseDirectory = db.getDatabaseLayout().databaseDirectory();
                try ( ResourceIterator<StoreFileMetadata> resourceIterator = files( request, db ) )
                {
                    while ( resourceIterator.hasNext() )
                    {
                        StoreFileMetadata storeFileMetadata = resourceIterator.next();
                        StoreResource storeResource = new StoreResource( storeFileMetadata.file(), relativePath( databaseDirectory, storeFileMetadata.file() ),
                                storeFileMetadata.recordSize(), fs );
                        storeFileStreamingProtocol.stream( ctx, storeResource );
                    }
                }
                lastCheckpointedTx = checkPointer.lastCheckPointedTransactionId();
                responseStatus = StoreCopyFinishedResponse.Status.SUCCESS;
            }
        }
        finally
        {
            storeFileStreamingProtocol.end( ctx, responseStatus, lastCheckpointedTx );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }

    abstract ResourceIterator<StoreFileMetadata> files( T request, Database database ) throws IOException;

    private static Iterator<StoreFileMetadata> onlyOne( List<StoreFileMetadata> files, String description )
    {
        if ( files.size() != 1 )
        {
            throw new IllegalStateException( format( "Expected exactly one file '%s'. Got %d", description, files.size() ) );
        }
        return files.iterator();
    }

    private static Predicate<StoreFileMetadata> matchesRequested( String fileName )
    {
        return f -> f.file().getName().equals( fileName );
    }

    private void tryAsyncCheckpoint( Database db, CheckPointer checkPointer )
    {
        db.getScheduler().schedule( Group.CHECKPOINT, () ->
        {
            try
            {
                checkPointer.tryCheckPointNoWait( new SimpleTriggerInfo( "Store file copy" ) );
            }
            catch ( IOException e )
            {
                log.error( "Failed to do a checkpoint that was invoked after a too far behind error on store copy request", e );
            }
        } );
    }

    public static class GetStoreFileRequestHandler extends StoreCopyRequestHandler<GetStoreFileRequest>
    {
        public GetStoreFileRequestHandler( CatchupServerProtocol protocol, Database db,
                StoreFileStreamingProtocol storeFileStreamingProtocol, FileSystemAbstraction fs, LogProvider logProvider )
        {
            super( protocol, db, storeFileStreamingProtocol, fs, logProvider, GetStoreFileRequest.class );
        }

        @Override
        ResourceIterator<StoreFileMetadata> files( GetStoreFileRequest request, Database database ) throws IOException
        {
            try ( ResourceIterator<StoreFileMetadata> resourceIterator = database.listStoreFiles( false ) )
            {
                String fileName = request.file().getName();
                return Iterators.asResourceIterator(
                        onlyOne( resourceIterator.stream().filter( matchesRequested( fileName ) ).collect( Collectors.toList() ), fileName ) );
            }
        }
    }

    public static class GetIndexSnapshotRequestHandler extends StoreCopyRequestHandler<GetIndexFilesRequest>
    {
        public GetIndexSnapshotRequestHandler( CatchupServerProtocol protocol, Database db,
                StoreFileStreamingProtocol storeFileStreamingProtocol, FileSystemAbstraction fs, LogProvider logProvider )
        {
            super( protocol, db, storeFileStreamingProtocol, fs, logProvider, GetIndexFilesRequest.class );
        }

        @Override
        ResourceIterator<StoreFileMetadata> files( GetIndexFilesRequest request, Database database ) throws IOException
        {
            return database.getDatabaseFileListing().getIndexFileListing().getSnapshot( request.indexId() );
        }
    }
}
