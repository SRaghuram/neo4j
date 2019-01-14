/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.causalclustering.helpers.Buffers;
import org.neo4j.causalclustering.messaging.NetworkWritableChannel;
import org.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;

public class TransactionRepresentationReplicatedTransactionTest
{
    @Rule
    public final Buffers buffers = new Buffers();

    @Test
    public void shouldMarshalToSameByteIfByteBufBackedOrNot() throws IOException
    {
        PhysicalTransactionRepresentation expectedTx =
                new PhysicalTransactionRepresentation( Collections.singleton( new Command.NodeCommand( new NodeRecord( 1 ), new NodeRecord( 2 ) ) ) );

        expectedTx.setHeader( new byte[0], 1, 2, 3, 4, 5, 6 );
        TransactionRepresentationReplicatedTransaction replicatedTransaction = ReplicatedTransaction.from( expectedTx );

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ByteBuf buffer = buffers.buffer();
        OutputStreamWritableChannel outputStreamWritableChannel = new OutputStreamWritableChannel( stream );
        NetworkWritableChannel networkWritableChannel = new NetworkWritableChannel( buffer );

        replicatedTransaction.marshal( outputStreamWritableChannel );
        replicatedTransaction.marshal( networkWritableChannel );

        byte[] bufferArray = Arrays.copyOf( buffer.array(), buffer.writerIndex() );

        Assertions.assertArrayEquals( bufferArray, stream.toByteArray() );
    }
}
