/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric;

import java.net.URI;
import java.util.List;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.fabric.executor.Location;

public class TestUtils
{
    public static Location.RemoteUri createUri( String uriString )
    {
        var uri = URI.create( uriString );
        return new Location.RemoteUri( uri.getScheme(), List.of( new SocketAddress( uri.getHost(), uri.getPort() ) ), uri.getQuery() );
    }
}
