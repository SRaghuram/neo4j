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

import java.nio.file.Path;
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
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( new Path[0], transactionId );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.lastCheckPointedTransactionId(), readPrepareStoreCopyResponse.lastCheckPointedTransactionId() );
    }

    @Test
    void fileListGetsTransmitted()
    {
        // given
        Path[] files =
                new Path[]{Path.of( "File a.txt" ), Path.of( "file-b" ), Path.of( "aoifnoasndfosidfoisndfoisnodainfsonidfaosiidfna" ), Path.of( "" )};

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( files, 0L );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.getPaths().length, readPrepareStoreCopyResponse.getPaths().length );
        for ( Path file : files )
        {
            assertEquals( 1,
                    Stream.of( readPrepareStoreCopyResponse.getPaths() ).map( Path::getFileName ).filter( f -> f.equals( file.getFileName() ) ).count() );
        }
    }

    @Test
    void descriptorsGetTransmitted()
    {
        // given
        Path[] files =
                new Path[]{Path.of( "File a.txt" ), Path.of( "file-b" ), Path.of( "aoifnoasndfosidfoisndfoisnodainfsonidfaosiidfna" ), Path.of( "" )};

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = PrepareStoreCopyResponse.success( files, 1L );
        sendToChannel( prepareStoreCopyResponse, embeddedChannel );

        // then it can be deserialised
        PrepareStoreCopyResponse readPrepareStoreCopyResponse = embeddedChannel.readInbound();
        assertEquals( prepareStoreCopyResponse.getPaths().length, readPrepareStoreCopyResponse.getPaths().length );
        for ( Path file : files )
        {
            assertEquals( 1,
                    Stream.of( readPrepareStoreCopyResponse.getPaths() ).map( Path::getFileName ).filter( f -> f.equals( file.getFileName() ) ).count() );
        }
    }

    private static void sendToChannel( PrepareStoreCopyResponse prepareStoreCopyResponse, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( prepareStoreCopyResponse );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
