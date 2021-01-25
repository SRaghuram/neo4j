/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.StateMachineResult;
import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.util.function.Consumer;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.LogEntryWriterFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

public abstract class ReplicatedTransaction implements CoreReplicatedContent
{
    private final DatabaseId databaseId;

    public ReplicatedTransaction( DatabaseId databaseId )
    {
        this.databaseId = databaseId;
    }

    public static TransactionRepresentationReplicatedTransaction from( TransactionRepresentation tx, NamedDatabaseId namedDatabaseId,
                                                                       LogEntryWriterFactory logEntryWriterFactory )
    {
        return new TransactionRepresentationReplicatedTransaction( tx, namedDatabaseId.databaseId(), logEntryWriterFactory );
    }

    public static ByteArrayReplicatedTransaction from( byte[] bytes, DatabaseId databaseId )
    {
        return new ByteArrayReplicatedTransaction( bytes, databaseId );
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<StateMachineResult> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    @Override
    public DatabaseId databaseId()
    {
        return databaseId;
    }

    public abstract ChunkedInput<ByteBuf> encode();

    public abstract TransactionRepresentation extract( TransactionRepresentationExtractor extractor );
}
