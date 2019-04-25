/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.messaging.EndOfStreamException;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.database.DatabaseId;

public class ReplicatedTokenRequestMarshalV2
{
    private ReplicatedTokenRequestMarshalV2()
    {
        throw new AssertionError( "Should not be instantiated" );
    }

    public static void marshal( ReplicatedTokenRequest tokenRequest, WritableChannel channel ) throws IOException
    {
        DatabaseIdMarshal.INSTANCE.marshal( tokenRequest.databaseId(), channel );
        channel.putInt( tokenRequest.type().ordinal() );
        StringMarshal.marshal( channel, tokenRequest.tokenName() );

        channel.putInt( tokenRequest.commandBytes().length );
        channel.put( tokenRequest.commandBytes(), tokenRequest.commandBytes().length );
    }

    public static ReplicatedTokenRequest unmarshal( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        DatabaseId databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( channel );
        TokenType type = TokenType.values()[channel.getInt()];
        String tokenName = StringMarshal.unmarshal( channel );

        int commandBytesLength = channel.getInt();
        byte[] commandBytes = new byte[commandBytesLength];
        channel.get( commandBytes, commandBytesLength );

        return new ReplicatedTokenRequest( databaseId, type, tokenName, commandBytes );
    }

    public static void marshal( ReplicatedTokenRequest content, ByteBuf buffer )
    {
        buffer.writeInt( content.type().ordinal() );
        StringMarshal.marshal( buffer, content.tokenName() );

        buffer.writeInt( content.commandBytes().length );
        buffer.writeBytes( content.commandBytes() );
    }

    public static ReplicatedTokenRequest unmarshal( ByteBuf buffer, DatabaseId databaseId )
    {
        TokenType type = TokenType.values()[buffer.readInt()];
        String tokenName = StringMarshal.unmarshal( buffer );

        int commandBytesLength = buffer.readInt();
        byte[] commandBytes = new byte[commandBytesLength];
        buffer.readBytes( commandBytes );

        return new ReplicatedTokenRequest( databaseId, type, tokenName, commandBytes );
    }
}
