/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.storageengine.api.StorageCommand;

import static org.neo4j.internal.kernel.api.security.AuthSubject.AUTH_DISABLED;

public class ReplicatedTransactionFactory
{
    private ReplicatedTransactionFactory()
    {
        throw new AssertionError( "Should not be instantiated" );
    }

    public static TransactionRepresentation extractTransactionRepresentation( ReplicatedTransaction transactionCommand, byte[] extraHeader,
            LogEntryReader reader )
    {
        return transactionCommand.extract( new TransactionRepresentationReader( extraHeader, reader ) );
    }

    static TransactionRepresentationWriter transactionalRepresentationWriter( TransactionRepresentation transactionCommand,
                                                                              LogEntryWriterFactory logEntryWriterFactory )
    {
        return new TransactionRepresentationWriter( transactionCommand, logEntryWriterFactory );
    }

    private static class TransactionRepresentationReader implements TransactionRepresentationExtractor
    {
        private final byte[] extraHeader;
        private final LogEntryReader reader;

        TransactionRepresentationReader( byte[] extraHeader, LogEntryReader reader )
        {
            this.extraHeader = extraHeader;
            this.reader = reader;
        }

        @Override
        public TransactionRepresentation extract( TransactionRepresentationReplicatedTransaction replicatedTransaction )
        {
            PhysicalTransactionRepresentation tx = (PhysicalTransactionRepresentation) replicatedTransaction.tx();
            tx.setAdditionalHeader( extraHeader );
            return tx;
        }

        @Override
        public TransactionRepresentation extract( ByteArrayReplicatedTransaction replicatedTransaction )
        {
            ByteBuf buffer = Unpooled.wrappedBuffer( replicatedTransaction.getTxBytes() );
            NetworkReadableChannel channel = new NetworkReadableChannel( buffer );
            return read( channel );
        }

        private TransactionRepresentation read( NetworkReadableChannel channel )
        {
            try
            {
                long latestCommittedTxWhenStarted = channel.getLong();
                long timeStarted = channel.getLong();
                long timeCommitted = channel.getLong();
                int leaseId = channel.getInt();

                int headerLength = channel.getInt();
                byte[] header;
                if ( headerLength == 0 )
                {
                    header = extraHeader;
                }
                else
                {
                    header = new byte[headerLength];
                }

                channel.get( header, headerLength );

                LogEntryCommand entryRead;
                List<StorageCommand> commands = new LinkedList<>();

                while ( (entryRead = (LogEntryCommand) reader.readLogEntry( channel )) != null )
                {
                    commands.add( entryRead.getCommand() );
                }

                PhysicalTransactionRepresentation tx = new PhysicalTransactionRepresentation( commands );
                tx.setHeader( header, timeStarted, latestCommittedTxWhenStarted, timeCommitted, leaseId, AUTH_DISABLED);

                return tx;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    static class TransactionRepresentationWriter
    {
        private final Iterator<StorageCommand> commands;
        private ThrowingConsumer<WritableChecksumChannel,IOException> nextJob;
        private LogEntryWriterFactory logEntryWriterFactory;

        private TransactionRepresentationWriter( TransactionRepresentation tx, LogEntryWriterFactory logEntryWriterFactory )
        {
            this.logEntryWriterFactory = logEntryWriterFactory;
            this.nextJob = channel ->
            {
                channel.putLong( tx.getLatestCommittedTxWhenStarted() );
                channel.putLong( tx.getTimeStarted() );
                channel.putLong( tx.getTimeCommitted() );
                channel.putInt( tx.getLeaseId() );

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
            this.commands = tx.iterator();
        }

        void write( WritableChecksumChannel channel ) throws IOException
        {
            nextJob.accept( channel );
            if ( commands.hasNext() )
            {
                StorageCommand storageCommand = commands.next();
                nextJob = c -> new LogEntryWriter<>( c, storageCommand.version() ).serialize( storageCommand );
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
