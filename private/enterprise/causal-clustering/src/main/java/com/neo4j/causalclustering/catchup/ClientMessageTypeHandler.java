/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class ClientMessageTypeHandler extends ChannelInboundHandlerAdapter
{
    private final Log log;
    private final CatchupClientProtocol protocol;

    public ClientMessageTypeHandler( CatchupClientProtocol protocol, LogProvider logProvider )
    {
        this.protocol = protocol;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        if ( protocol.isExpecting( CatchupClientProtocol.State.MESSAGE_TYPE ) )
        {
            byte byteValue = ((ByteBuf) msg).readByte();
            ResponseMessageType responseMessageType = ResponseMessageType.from( byteValue );

            switch ( responseMessageType )
            {
            case STORE_ID:
                protocol.expect( CatchupClientProtocol.State.STORE_ID );
                break;
            case TX:
                protocol.expect( CatchupClientProtocol.State.TX_PULL_RESPONSE );
                break;
            case FILE:
                protocol.expect( CatchupClientProtocol.State.FILE_HEADER );
                break;
            case STORE_COPY_FINISHED:
                protocol.expect( CatchupClientProtocol.State.STORE_COPY_FINISHED );
                break;
            case CORE_SNAPSHOT:
                protocol.expect( CatchupClientProtocol.State.CORE_SNAPSHOT );
                break;
            case TX_STREAM_FINISHED:
                protocol.expect( CatchupClientProtocol.State.TX_STREAM_FINISHED );
                break;
            case PREPARE_STORE_COPY_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.PREPARE_STORE_COPY_RESPONSE );
                break;
            case INDEX_SNAPSHOT_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.INDEX_SNAPSHOT_RESPONSE );
                break;
            case ERROR:
                protocol.expect( CatchupClientProtocol.State.ERROR_RESPONSE );
                break;
            case DATABASE_ID_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.DATABASE_ID );
                break;
            case ALL_DATABASE_IDS_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.GET_ALL_DATABASE_IDS );
                break;
            case INFO_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.GET_INFO );
                break;
            case METADATA_RESPONSE:
                protocol.expect( CatchupClientProtocol.State.GET_METADATA );
                break;
            default:
                log.warn( "No handler found for message type %s (%d)", responseMessageType.name(), byteValue );
            }

            ReferenceCountUtil.release( msg );
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }
}
