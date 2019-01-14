/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.IOException;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;

/**
 * Serialized {@link CommittedTransactionRepresentation transactions} to raw bytes on the {@link ChannelBuffer
 * network}.
 * One serializer can be instantiated per response and is able to serialize one or many transactions.
 */
public class CommittedTransactionSerializer implements Visitor<CommittedTransactionRepresentation,Exception>
{
    private final LogEntryWriter writer;

    public CommittedTransactionSerializer( FlushableChannel networkFlushableChannel )
    {
        this.writer = new LogEntryWriter( networkFlushableChannel );
    }

    @Override
    public boolean visit( CommittedTransactionRepresentation tx ) throws IOException
    {
        writer.serialize( tx );
        return false;
    }
}
