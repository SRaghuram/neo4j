/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.causalclustering.helper.ErrorHandler;
import org.neo4j.causalclustering.messaging.BoundedNetworkChannel;
import org.neo4j.causalclustering.messaging.marshalling.ChunkedEncoder;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.StorageCommandSerializer;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.WritableChannel;

import static java.lang.Math.min;

public class TransactionRepresentationReplicatedTransaction implements ReplicatedTransaction
{
    private final TransactionRepresentation tx;

    TransactionRepresentationReplicatedTransaction( TransactionRepresentation tx )
    {
        this.tx = tx;
    }

    @Override
    public ChunkedEncoder marshal()
    {
        return new TxRepresentationMarshal( tx );
    }

    public TransactionRepresentation tx()
    {
        return tx;
    }

    public class TxRepresentationMarshal implements ChunkedEncoder
    {
        private final TxRepresentationMarshal.TransactionRepresentationWriter txWriter;
        private BoundedNetworkChannel channel;
        private Queue<ByteBuf> output = new LinkedList<>();

        private TxRepresentationMarshal( TransactionRepresentation tx )
        {
            txWriter = new TxRepresentationMarshal.TransactionRepresentationWriter( tx );
        }

        @Override
        public ByteBuf encodeChunk( ByteBufAllocator allocator ) throws IOException
        {
            if ( isEndOfInput() )
            {
                return null;
            }
            if ( channel == null )
            {
                // Ensure that the written buffers does not overflow the allocators chunk size.
                int maxChunkSize = getMaxChunkSize( allocator );
                channel = new BoundedNetworkChannel( allocator, maxChunkSize, output );
                // Unknown length
                channel.putInt( -1 );
            }
            try
            {
                // check if there are chunks to send
                if ( !output.isEmpty() )
                {
                    return output.poll();
                }
                // otherwise continue writing
                if ( txWriter.canWrite() )
                {
                    while ( txWriter.canWrite() && output.isEmpty() )
                    {
                        txWriter.write( channel );
                    }
                }
                // nothing more to write, flush latest chunk and close channel
                if ( output.isEmpty() )
                {
                    channel.prepareForFlush().flush();
                    channel.close();
                }
                return output.poll();
            }
            catch ( Throwable t )
            {
                try
                {
                    channel.close();
                }
                catch ( IOException e )
                {
                    t.addSuppressed( e );
                }
                try
                {
                    ErrorHandler.runAll( "releasing buffers",
                            output.stream().map( b -> (ErrorHandler.ThrowingRunnable) b::release ).toArray( ErrorHandler.ThrowingRunnable[]::new ) );
                }
                catch ( Exception e )
                {
                    t.addSuppressed( e );
                }
                throw t;
            }
        }

        private int getMaxChunkSize( ByteBufAllocator allocator )
        {
            int allocatorChunkSize = Integer.MAX_VALUE;
            if ( allocator instanceof PooledByteBufAllocator )
            {
                allocatorChunkSize = ((PooledByteBufAllocator) allocator).metric().chunkSize() / 4;
            }
            return min( (int) ByteUnit.mebiBytes( 1 ), allocatorChunkSize );
        }

        @Override
        public void marshal( WritableChannel channel ) throws IOException
        {
            // Unknown length - this does not play well with RaftV1 which requires a size. Needs special care if used in raft-v1 to set size after marshalling
            channel.putInt( -1 );
            while ( txWriter.canWrite() )
            {
                txWriter.write( channel );
            }
        }

        @Override
        public boolean isEndOfInput()
        {
            return channel != null && channel.closed() && output.isEmpty();
        }

        private class TransactionRepresentationWriter
        {
            private final Iterator<StorageCommand> iterator;
            private ThrowingConsumer<WritableChannel,IOException> nextJob;

            private TransactionRepresentationWriter( TransactionRepresentation tx )
            {
                nextJob = channel ->
                {
                    channel.putInt( tx.getAuthorId() );
                    channel.putInt( tx.getMasterId() );
                    channel.putLong( tx.getLatestCommittedTxWhenStarted() );
                    channel.putLong( tx.getTimeStarted() );
                    channel.putLong( tx.getTimeCommitted() );
                    channel.putInt( tx.getLockSessionId() );

                    byte[] additionalHeader = tx.additionalHeader();
                    if ( additionalHeader != null )
                    {
                        channel.putInt( additionalHeader.length );
                        channel.put( additionalHeader, additionalHeader.length );
                    }
                    else
                    {
                        channel.putInt( 0 );
                    }
                };
                iterator = tx.iterator();
            }

            void write( WritableChannel channel ) throws IOException
            {
                nextJob.accept( channel );
                if ( iterator.hasNext() )
                {
                    StorageCommand storageCommand = iterator.next();
                    nextJob = c -> new StorageCommandSerializer( c ).visit( storageCommand );
                }
                else
                {
                    nextJob = null;
                }
            }

            boolean canWrite()
            {
                return nextJob != null;
            }
        }
    }
}
