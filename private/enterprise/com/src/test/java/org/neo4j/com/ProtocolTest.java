/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.ReadableClosablePositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.storageengine.api.StorageCommand;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProtocolTest
{
    @Test
    public void shouldSerializeAndDeserializeTransactionRepresentation() throws Exception
    {
        // GIVEN
        PhysicalTransactionRepresentation transaction = new PhysicalTransactionRepresentation( justOneNode() );
        byte[] additionalHeader = "extra".getBytes();
        int masterId = 1;
        int authorId = 2;
        long timeStarted = 12345;
        long lastTxWhenStarted = 12;
        long timeCommitted = timeStarted + 10;
        transaction.setHeader( additionalHeader, masterId, authorId, timeStarted, lastTxWhenStarted, timeCommitted, -1 );
        Protocol.TransactionSerializer serializer = new Protocol.TransactionSerializer( transaction );
        ChannelBuffer buffer = new ChannelBufferWrapper( new InMemoryClosableChannel() );

        // WHEN serializing the transaction
        serializer.write( buffer );

        // THEN deserializing the same transaction should yield the same data.
        // ... remember that this deserializer doesn't read the data source name string. Read it manually here
        assertEquals( NeoStoreDataSource.DEFAULT_DATA_SOURCE_NAME, Protocol.readString( buffer ) );
        VersionAwareLogEntryReader<ReadableClosablePositionAwareChannel> reader = new VersionAwareLogEntryReader<>();
        TransactionRepresentation readTransaction = new Protocol.TransactionRepresentationDeserializer( reader )
                .read( buffer, ByteBuffer.allocate( 1000 ) );
        assertArrayEquals( additionalHeader, readTransaction.additionalHeader() );
        assertEquals( masterId, readTransaction.getMasterId() );
        assertEquals( authorId, readTransaction.getAuthorId() );
        assertEquals( timeStarted, readTransaction.getTimeStarted() );
        assertEquals( lastTxWhenStarted, readTransaction.getLatestCommittedTxWhenStarted() );
        assertEquals( timeCommitted, readTransaction.getTimeCommitted() );
    }

    private Collection<StorageCommand> justOneNode()
    {
        NodeRecord node = new NodeRecord( 0 );
        node.setInUse( true );
        return Arrays.asList( new NodeCommand( new NodeRecord( node.getId() ), node ) );
    }
}
