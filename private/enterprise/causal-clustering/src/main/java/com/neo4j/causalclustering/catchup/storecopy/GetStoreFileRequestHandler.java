/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.logging.Log;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.util.VisibleForTesting;

import static java.lang.String.format;

public class GetStoreFileRequestHandler extends SimpleChannelInboundHandler<GetStoreFileRequest>
{
    private final CatchupServerProtocol protocol;
    private final Database db;
    private final StoreFileStreamingProtocol storeFileStreamingProtocol;

    private final FileSystemAbstraction fs;
    private final Log log;

    public GetStoreFileRequestHandler( CatchupServerProtocol protocol, Database db, StoreFileStreamingProtocol storeFileStreamingProtocol,
            FileSystemAbstraction fs )
    {
        this.protocol = protocol;
        this.db = db;
        this.storeFileStreamingProtocol = storeFileStreamingProtocol;
        this.fs = fs;
        this.log = db.getInternalLogProvider().getLog( getClass() );
    }

    @Override
    protected final void channelRead0( ChannelHandlerContext ctx, GetStoreFileRequest request ) throws Exception
    {
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
                Path databaseDirectory = db.getDatabaseLayout().databaseDirectory();
                try ( ResourceIterator<StoreFileMetadata> resourceIterator = files( request, db ) )
                {
                    while ( resourceIterator.hasNext() )
                    {
                        StoreFileMetadata storeFileMetadata = resourceIterator.next();
                        StoreResource storeResource =
                                new StoreResource( storeFileMetadata.path(), databaseDirectory.relativize( storeFileMetadata.path() ).toString(),
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

    @VisibleForTesting
    ResourceIterator<StoreFileMetadata> files( GetStoreFileRequest request, Database database ) throws IOException
    {
        try ( ResourceIterator<StoreFileMetadata> resourceIterator = database.listStoreFiles( false ) )
        {
            String fileName = request.path().getFileName().toString();
            return Iterators.asResourceIterator(
                    onlyOne( resourceIterator.stream().filter( matchesRequested( fileName ) ).collect( Collectors.toList() ), fileName ) );
        }
    }

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
        return f -> f.path().getFileName().toString().equals( fileName );
    }

    private void tryAsyncCheckpoint( Database db, CheckPointer checkPointer )
    {
        var monitoringParams = JobMonitoringParams.systemJob( db.getNamedDatabaseId().name(), "Checkpoint triggered by store file copy" );
        db.getScheduler().schedule( Group.CHECKPOINT, monitoringParams, () ->
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
}
