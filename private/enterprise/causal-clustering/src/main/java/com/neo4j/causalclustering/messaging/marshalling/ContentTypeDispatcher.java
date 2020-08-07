/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.catchup.Protocol;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;

public class ContentTypeDispatcher extends ChannelInboundHandlerAdapter
{
    private final Protocol<ContentType> contentTypeProtocol;

    public ContentTypeDispatcher( Protocol<ContentType> contentTypeProtocol )
    {
        this.contentTypeProtocol = contentTypeProtocol;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg )
    {
        if ( msg instanceof ByteBuf )
        {
            ByteBuf buffer = (ByteBuf) msg;
            if ( contentTypeProtocol.isExpecting( ContentType.ContentType ) )
            {
                byte messageCode = buffer.readByte();
                ContentType contentType = getContentType( messageCode );
                contentTypeProtocol.expect( contentType );
                if ( buffer.readableBytes() == 0 )
                {
                    ReferenceCountUtil.release( msg );
                    return;
                }
            }
        }
        ctx.fireChannelRead( msg );
    }

    private ContentType getContentType( byte messageCode )
    {
        for ( ContentType contentType : ContentType.values() )
        {
            if ( contentType.get() == messageCode )
            {
                return contentType;
            }
        }
        throw new IllegalArgumentException( "Illegal inbound. Could not find a ContentType with value " + messageCode );
    }
}
