/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.helpers.Buffers;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.OutputStreamWritableChannel;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import org.neo4j.internal.recordstorage.Command;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@Buffers.Extension
class TransactionRepresentationReplicatedTransactionTest
{
    @Inject
    private Buffers buffers;

    @Test
    void shouldMarshalToSameByteIfByteBufBackedOrNot() throws IOException
    {
        var expectedTx =
                new PhysicalTransactionRepresentation( Collections.singleton( new Command.NodeCommand( new NodeRecord( 1 ), new NodeRecord( 2 ) ) ) );

        expectedTx.setHeader( new byte[0], 3, 4, 5, 6 );
        var replicatedTransaction = ReplicatedTransaction.from( expectedTx, new TestDatabaseIdRepository().defaultDatabase() );

        var stream = new ByteArrayOutputStream();
        var buffer = buffers.buffer();
        var outputStreamWritableChannel = new OutputStreamWritableChannel( stream );
        var networkWritableChannel = new NetworkWritableChannel( buffer );

        ReplicatedTransactionMarshalV2.marshal( outputStreamWritableChannel, replicatedTransaction );
        ReplicatedTransactionMarshalV2.marshal( networkWritableChannel, replicatedTransaction );

        var bufferArray = Arrays.copyOf( buffer.array(), buffer.writerIndex() );

        assertArrayEquals( bufferArray, stream.toByteArray() );
    }
}
