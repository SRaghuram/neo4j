/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.LocalOperationId;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.marshalling.ContentBuilder;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentHandler;

import java.io.IOException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.UUID;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.EndOfStreamException;

/**
 * A uniquely identifiable operation.
 */
public class DistributedOperation implements ReplicatedContent
{
    private final ReplicatedContent content;
    private final GlobalSession globalSession;
    private final LocalOperationId operationId;

    public DistributedOperation( ReplicatedContent content, GlobalSession globalSession, LocalOperationId operationId )
    {
        this.content = content;
        this.globalSession = globalSession;
        this.operationId = operationId;
    }

    public GlobalSession globalSession()
    {
        return globalSession;
    }

    public LocalOperationId operationId()
    {
        return operationId;
    }

    public ReplicatedContent content()
    {
        return content;
    }

    @Override
    public OptionalLong size()
    {
        return content.size();
    }

    @Override
    public void dispatch( ReplicatedContentHandler contentHandler ) throws IOException
    {
        contentHandler.handle( this );
        content().dispatch( contentHandler );
    }

    /**
     * This this consumer ignores the content which is handles by its own serializer.
     */
    public void marshalMetaData( WritableChannel channel ) throws IOException
    {
        channel.putLong( globalSession().sessionId().getMostSignificantBits() );
        channel.putLong( globalSession().sessionId().getLeastSignificantBits() );
        new MemberId.Marshal().marshal( globalSession().owner(), channel );

        channel.putLong( operationId.localSessionId() );
        channel.putLong( operationId.sequenceNumber() );
    }

    public static ContentBuilder<ReplicatedContent> deserialize( ReadableChannel channel ) throws IOException, EndOfStreamException
    {
        long mostSigBits = channel.getLong();
        long leastSigBits = channel.getLong();
        MemberId owner = new MemberId.Marshal().unmarshal( channel );
        GlobalSession globalSession = new GlobalSession( new UUID( mostSigBits, leastSigBits ), owner );

        long localSessionId = channel.getLong();
        long sequenceNumber = channel.getLong();
        LocalOperationId localOperationId = new LocalOperationId( localSessionId, sequenceNumber );

        return ContentBuilder.unfinished( subContent -> new DistributedOperation( subContent, globalSession, localOperationId ) );
    }

    @Override
    public String toString()
    {
        return "DistributedOperation{" +
               "content=" + content +
               ", globalSession=" + globalSession +
               ", operationId=" + operationId +
               '}';
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
        DistributedOperation that = (DistributedOperation) o;
        return Objects.equals( content, that.content ) && Objects.equals( globalSession, that.globalSession ) &&
                Objects.equals( operationId, that.operationId );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( content, globalSession, operationId );
    }
}
