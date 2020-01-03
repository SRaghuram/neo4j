/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;

public interface RemoteMembersResolver
{
    default <REMOTE> Collection<REMOTE> resolve( Function<SocketAddress,REMOTE> transform )
    {
        return resolve( transform, ArrayList::new );
    }

    <COLL extends Collection<REMOTE>,REMOTE> COLL resolve( Function<SocketAddress,REMOTE> transform, Supplier<COLL> collectionFactory );

    boolean useOverrides();
}
