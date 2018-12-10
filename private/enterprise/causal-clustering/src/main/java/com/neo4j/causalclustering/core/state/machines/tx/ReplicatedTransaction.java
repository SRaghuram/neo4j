/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.Result;
import io.netty.buffer.ByteBuf;
import io.netty.handler.stream.ChunkedInput;

import java.util.function.Consumer;

import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

public abstract class ReplicatedTransaction implements CoreReplicatedContent
{
    private final String databaseName;

    public ReplicatedTransaction( String databaseName )
    {
        this.databaseName = databaseName;
    }

    public static TransactionRepresentationReplicatedTransaction from( TransactionRepresentation tx, String databaseName )
    {
        return new TransactionRepresentationReplicatedTransaction( tx, databaseName );
    }

    public static ByteArrayReplicatedTransaction from( byte[] bytes, String databaseName )
    {
        return new ByteArrayReplicatedTransaction( bytes, databaseName );
    }

    @Override
    public void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback )
    {
        commandDispatcher.dispatch( this, commandIndex, callback );
    }

    @Override
    public String databaseName()
    {
        return databaseName;
    }

    public abstract ChunkedInput<ByteBuf> encode();

    public abstract TransactionRepresentation extract( TransactionRepresentationExtractor extractor );
}
