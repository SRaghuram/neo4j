/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.v2.storecopy.GetStoreFileRequestMarshalV2;
import org.neo4j.causalclustering.identity.StoreId;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@RunWith( Parameterized.class  )
public class GetStoreFileMarshalTest
{
    EmbeddedChannel embeddedChannel;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
                {
                        { "[Marshal V1]", new GetStoreFileRequest.Encoder(), new GetStoreFileRequest.Decoder( DEFAULT_DATABASE_NAME ) },
                        { "[Marshal V2]", new GetStoreFileRequestMarshalV2.Encoder(), new GetStoreFileRequestMarshalV2.Decoder() }
                }
        );
    }

    public GetStoreFileMarshalTest( String ignored, MessageToByteEncoder<GetStoreFileRequest> encoder, ByteToMessageDecoder decoder )
    {
        embeddedChannel = new EmbeddedChannel( encoder, decoder );
    }

    private static final StoreId expectedStore = new StoreId( 1, 2, 3, 4 );
    private static final File expectedFile = new File( "abc.123" );
    private static final Long expectedLastTransaction = 1234L;

    @Test
    public void getsTransmitted()
    {
        // given
        GetStoreFileRequest expectedStoreRequest = new GetStoreFileRequest( expectedStore, expectedFile, expectedLastTransaction, DEFAULT_DATABASE_NAME );

        // when
        sendToChannel( expectedStoreRequest, embeddedChannel );

        // then
        GetStoreFileRequest actualStoreRequest = embeddedChannel.readInbound();
        assertEquals( expectedStore, actualStoreRequest.expectedStoreId() );
        assertEquals( expectedFile, actualStoreRequest.file() );
        assertEquals( expectedLastTransaction.longValue(), actualStoreRequest.requiredTransactionId() );
    }

    private static void sendToChannel( GetStoreFileRequest getStoreFileRequest, EmbeddedChannel embeddedChannel )
    {
        embeddedChannel.writeOutbound( getStoreFileRequest );

        ByteBuf object = embeddedChannel.readOutbound();
        embeddedChannel.writeInbound( object );
    }
}
