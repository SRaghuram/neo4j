/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.protocol.init.InitMagicMessageDecoder.State.ILLEGAL_MESSAGE;
import static com.neo4j.causalclustering.protocol.init.InitMagicMessageDecoder.State.READ_MESSAGE_BODY;
import static com.neo4j.causalclustering.protocol.init.InitMagicMessageDecoder.State.READ_MESSAGE_CODE;

class InitMagicMessageDecoder extends ReplayingDecoder<InitMagicMessageDecoder.State>
{
    static final String NAME = "init_magic_message_decoder";

    private final Log log;

    InitMagicMessageDecoder( LogProvider logProvider )
    {
        super( State.READ_MESSAGE_CODE );
        setSingleDecode( true );
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        var currentState = state();

        switch ( currentState )
        {
        case READ_MESSAGE_CODE:
            readMessageCode( ctx, in );
            // no break on purpose - try to read both code and body in one go
        case READ_MESSAGE_BODY:
            var message = readMessage( in );
            out.add( message );
            break;
        case ILLEGAL_MESSAGE:
            // keep discarding until disconnection
            in.skipBytes( actualReadableBytes() );
            break;
        default:
            throw new IllegalStateException( "Unknown decoder state: " + currentState );
        }
    }

    private void readMessageCode( ChannelHandlerContext ctx, ByteBuf in )
    {
        var code = in.readInt();
        if ( code != InitialMagicMessage.MESSAGE_CODE )
        {
            checkpoint( ILLEGAL_MESSAGE );
            ctx.close();
            throw new DecoderException( "Illegal initialization message code: " + code );
        }
        else
        {
            checkpoint( READ_MESSAGE_BODY );
            log.debug( "Channel %s received a correct initialization message code", ctx.channel() );
        }
    }

    private InitialMagicMessage readMessage( ByteBuf in )
    {
        var magic = StringMarshal.unmarshal( in );
        var message = new InitialMagicMessage( magic );
        checkpoint( READ_MESSAGE_CODE );
        return message;
    }

    enum State
    {
        READ_MESSAGE_CODE,
        READ_MESSAGE_BODY,
        ILLEGAL_MESSAGE
    }
}
