/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.LAST_CHECKPOINTED_TX_UNAVAILABLE;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.SUCCESS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith( EphemeralFileSystemExtension.class )
class StoreFileStreamingProtocolTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;

    private final int maxChunkSize = 32768;

    @Test
    void shouldStreamResources() throws Exception
    {
        // given
        var protocol = new StoreFileStreamingProtocol( maxChunkSize );
        var ctx = mock( ChannelHandlerContext.class );

        fs.mkdir( new File( "dirA" ) );
        fs.mkdir( new File( "dirB" ) );

        String[] files = new String[]{"dirA/one", "dirA/two", "dirB/one", "dirB/two", "one", "two", "three"};

        var resourceList = new ArrayList<StoreResource>();
        for ( var file : files )
        {
            resourceList.add( createResource( Path.of( file ), ThreadLocalRandom.current().nextInt( 1, 4096 ) ) );
        }

        // when
        for ( var storeResource : resourceList )
        {
            protocol.stream( ctx, storeResource );
        }

        // then
        var inOrder = Mockito.inOrder( ctx );

        for ( var resource : resourceList )
        {
            inOrder.verify( ctx ).write( ResponseMessageType.FILE );
            inOrder.verify( ctx ).write( new FileHeader( resource.relativePath(), resource.recordSize() ) );
            inOrder.verify( ctx ).write( new FileSender( resource, 32768 ) );
        }
        verifyNoMoreInteractions( ctx );
    }

    @Test
    void shouldBeAbleToEndWithFailure()
    {
        // given
        var protocol = new StoreFileStreamingProtocol( maxChunkSize );
        var ctx = mock( ChannelHandlerContext.class );

        // when
        protocol.end( ctx, E_STORE_ID_MISMATCH, -1 );

        // then
        var inOrder = Mockito.inOrder( ctx );
        inOrder.verify( ctx ).write( ResponseMessageType.STORE_COPY_FINISHED );
        inOrder.verify( ctx ).writeAndFlush( new StoreCopyFinishedResponse( E_STORE_ID_MISMATCH, LAST_CHECKPOINTED_TX_UNAVAILABLE ) );
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldBeAbleToEndWithSuccess()
    {
        // given
        var protocol = new StoreFileStreamingProtocol( maxChunkSize );
        var ctx = mock( ChannelHandlerContext.class );

        // when
        protocol.end( ctx, StoreCopyFinishedResponse.Status.SUCCESS, -1 );

        // then
        var inOrder = Mockito.inOrder( ctx );
        inOrder.verify( ctx ).write( ResponseMessageType.STORE_COPY_FINISHED );
        inOrder.verify( ctx ).writeAndFlush( new StoreCopyFinishedResponse( SUCCESS, LAST_CHECKPOINTED_TX_UNAVAILABLE ) );
        inOrder.verifyNoMoreInteractions();
    }

    private StoreResource createResource( Path file, int recordSize ) throws IOException
    {
        fs.write( file.toFile() );
        return new StoreResource( file, file.toAbsolutePath().toString(), recordSize, fs );
    }
}
