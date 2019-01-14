/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.token;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageCommandReaderFactory;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.InvalidLogEntryHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.WritableChannel;

public class ReplicatedTokenRequestMarshalV1
{
    private ReplicatedTokenRequestMarshalV1()
    {
        throw new AssertionError( "Should not be instantiated" );
    }

    public static void marshal( ReplicatedTokenRequest content, WritableChannel channel ) throws IOException
    {
        channel.putInt( content.type().ordinal() );
        StringMarshal.marshal( channel, content.tokenName() );

        channel.putInt( content.commandBytes().length );
        channel.put( content.commandBytes(), content.commandBytes().length );
    }

    public static ReplicatedTokenRequest unmarshal( ReadableChannel channel, String databaseName ) throws IOException
    {
        TokenType type = TokenType.values()[ channel.getInt() ];
        String tokenName = StringMarshal.unmarshal( channel );

        int commandBytesLength = channel.getInt();
        byte[] commandBytes = new byte[ commandBytesLength ];
        channel.get( commandBytes, commandBytesLength );

        return new ReplicatedTokenRequest( databaseName, type, tokenName, commandBytes );
    }

    public static void marshal( ReplicatedTokenRequest content, ByteBuf buffer )
    {
        buffer.writeInt( content.type().ordinal() );
        StringMarshal.marshal( buffer, content.tokenName() );

        buffer.writeInt( content.commandBytes().length );
        buffer.writeBytes( content.commandBytes() );
    }

    public static ReplicatedTokenRequest unmarshal( ByteBuf buffer, String databaseName )
    {
        TokenType type = TokenType.values()[ buffer.readInt() ];
        String tokenName = StringMarshal.unmarshal( buffer );

        int commandBytesLength = buffer.readInt();
        byte[] commandBytes = new byte[ commandBytesLength ];
        buffer.readBytes( commandBytes );

        return new ReplicatedTokenRequest( databaseName, type, tokenName, commandBytes );
    }
}
