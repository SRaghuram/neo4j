/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.com.message;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.helpers.collection.Iterables;

public class TrackingMessageHolder implements MessageHolder
{
    private final List<Message> messages = new ArrayList<>();

    @Override
    public void offer( Message<? extends MessageType> message )
    {
        messages.add( message );
    }

    public <T extends MessageType> Message<T> single()
    {
        return Iterables.single( messages );
    }

    public <T extends MessageType> Message<T> first()
    {
        return Iterables.first( messages );
    }
}
