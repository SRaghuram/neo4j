/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import com.neo4j.causalclustering.messaging.BoundedNetworkWritableChannel;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

public class StorageCommandMarshal
{
    public static byte[] commandsToBytes( Collection<StorageCommand> commands, LogEntryWriterFactory logEntryWriterFactory )
    {
        ByteBuf commandBuffer = Unpooled.buffer();
        BoundedNetworkWritableChannel channel = new BoundedNetworkWritableChannel( commandBuffer );

        try
        {
            logEntryWriterFactory.createEntryWriter( channel ).serialize( commands );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "This should not be possible since it is all in memory." );
        }

        /*
         * This trims down the array to send up to the actual index it was written. Not doing this would send additional
         * zeroes which not only wasteful, but also not handled by the LogEntryReader receiving this.
         */
        byte[] commandsBytes = Arrays.copyOf( commandBuffer.array(), commandBuffer.writerIndex() );
        commandBuffer.release();

        return commandsBytes;
    }

    static Collection<StorageCommand> bytesToCommands( byte[] commandBytes, CommandReaderFactory commandReaderFactory )
    {
        ByteBuf txBuffer = Unpooled.wrappedBuffer( commandBytes );
        NetworkReadableChannel channel = new NetworkReadableChannel( txBuffer );

        LogEntryReader reader = new VersionAwareLogEntryReader( commandReaderFactory );

        LogEntryCommand entryRead;
        List<StorageCommand> commands = new LinkedList<>();

        try
        {
            while ( (entryRead = (LogEntryCommand) reader.readLogEntry( channel )) != null )
            {
                commands.add( entryRead.getCommand() );
            }
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "This should not be possible since it is all in memory." );
        }

        return commands;
    }
}
