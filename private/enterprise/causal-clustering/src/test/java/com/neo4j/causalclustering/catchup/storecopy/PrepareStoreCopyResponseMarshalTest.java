/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PrepareStoreCopyResponseMarshalTest
{
    private EmbeddedChannel embeddedChannel;

    @BeforeEach
    public void setup()
    {
        embeddedChannel = new EmbeddedChannel( new PrepareStoreCopyResponse.Encoder(), new PrepareStoreCopyResponse.Decoder() );
    }

    @Test
    void transactionIdGetsTransmitted()
    {
        // given
        long transactionId = Long.MAX_VALUE;

        // when a transaction id is serialised
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( new File[0], transactionId );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.lastCheckPointedTransactionId(), readPrepareStoreCopyResponse.lastCheckPointedTransactionId() );
    }

    @Test
    void fileListGetsTransmitted()
    {
        // given
        File[] files =
                new File[]{new File( "File a.txt" ), new File( "file-b" ), new File( "aoifnoasndfosidfoisndfoisnodainfsonidfaosiidfna" ), new File( "" )};

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( files, 0L );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.getFiles().length, readPrepareStoreCopyResponse.getFiles().length );
        for ( File file : files )
        {
            assertEquals( 1, Stream.of( readPrepareStoreCopyResponse.getFiles() ).map( File::getName ).filter( f -> f.equals( file.getName() ) ).count() );
        }
    }

    @Test
    void descriptorsGetTransmitted()
    {
        // given
        File[] files =
                new File[]{new File( "File a.txt" ), new File( "file-b" ), new File( "aoifnoasndfosidfoisndfoisnodainfsonidfaosiidfna" ), new File( "" )};

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( files, 1L );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.getFiles().length, readPrepareStoreCopyResponse.getFiles().length );
        for ( File file : files )
        {
            assertEquals( 1, Stream.of( readPrepareStoreCopyResponse.getFiles() ).map( File::getName ).filter( f -> f.equals( file.getName() ) ).count() );
        }
    }

    private static void sendToChannel( PrepareStoreCopyResponse prepareStoreCopyResponse, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( prepareStoreCopyResponse );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
