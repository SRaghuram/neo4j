/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.causalclustering.discovery.ClientConnector;
import org.neo4j.helpers.AdvertisedSocketAddress;

import static java.util.Collections.emptyList;

public class Util
{
    private Util()
    {
    }

    public static <T> List<T> asList( @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" ) Optional<T> optional )
    {
        return optional.map( Collections::singletonList ).orElse( emptyList() );
    }

    public static Function<ClientConnector,AdvertisedSocketAddress> extractBoltAddress()
    {
        return c -> c.connectors().boltAddress();
    }
}
