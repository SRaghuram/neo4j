/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.system;

import java.util.Deque;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Static class for ensuring that we use unique RemoteInstrumentIdentifiers
 */
public class AkkaRemoteInstrumentIdentifiers
{
    private static final Deque<Byte> ids = IntStream.range( 8, 32 ).boxed().map( Integer::byteValue ).collect( Collectors.toCollection( LinkedList::new ) );

    /**
     * This should only be called once (statically) for each RemoteInstrument class.
     *
     * @return
     */
    public static synchronized byte getIdentifier()
    {
        return ids.pop();
    }
}
