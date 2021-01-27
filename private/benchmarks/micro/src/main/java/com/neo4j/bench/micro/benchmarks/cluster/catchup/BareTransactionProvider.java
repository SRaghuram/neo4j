/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.StorageCommand;

class BareTransactionProvider implements Iterator<CommittedTransactionRepresentation>
{
    private int limit;
    private Random rnd = new Random();
    private List<StorageCommand> commands = List.of();

    private int counter;

    void set( int txSize, int limit )
    {
        this.limit = limit;
        rnd = new Random();
        commands = limit > 0 ? prefillCommands( txSize ) : List.of();
    }

    List<StorageCommand> prefillCommands( int txSize )
    {
        var commandCount = txSize / 64;
        var commands = new ArrayList<StorageCommand>( commandCount );
        byte[] bytes;
        for ( var i = 0; i < commandCount; i++ )
        {
            bytes = new byte[64 - 1];
            rnd.nextBytes( bytes );
            commands.add( new TestCommand( bytes ) );
        }
        return commands;
    }

    boolean reset()
    {
        counter = 0;
        return limit > 0;
    }

    public boolean hasNext()
    {
        return counter < limit;
    }

    public CommittedTransactionRepresentation next()
    {
        counter++;
        var startEntry = new LogEntryStart( System.currentTimeMillis(), counter, rnd.nextInt(), new byte[0], LogPosition.UNSPECIFIED );
        var txRep = new PhysicalTransactionRepresentation( commands );
        var commitEntry = new LogEntryCommit( counter + 1, System.currentTimeMillis(), rnd.nextInt() );
        return new CommittedTransactionRepresentation( startEntry, txRep, commitEntry );
    }

    static final CommandReaderFactory COMMAND_FACTORY = new CommandReaderFactory()
    {
        private final CommandReader READER = channel ->
        {
            int length = channel.getInt();
            byte[] bytes = new byte[length];
            channel.get( bytes, length );
            return new TestCommand( bytes );
        };

        @Override
        public CommandReader get( KernelVersion version )
        {
            return READER;
        }
    };

    static class TestCommand implements StorageCommand
    {
        private final byte[] bytes;

        TestCommand( byte[] bytes )
        {
            this.bytes = bytes;
        }

        @Override
        public void serialize( WritableChannel channel ) throws IOException
        {
            channel.putInt( bytes.length );
            channel.put( bytes, bytes.length );
        }

        @Override
        public KernelVersion version()
        {
            return KernelVersion.LATEST;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            TestCommand that = (TestCommand) o;
            return Arrays.equals( bytes, that.bytes );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( bytes );
        }
    }
}
