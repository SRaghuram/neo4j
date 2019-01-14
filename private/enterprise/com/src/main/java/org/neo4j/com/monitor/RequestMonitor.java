/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.monitor;

import java.net.SocketAddress;

import org.neo4j.com.RequestContext;
import org.neo4j.com.RequestType;

public interface RequestMonitor
{
    void beginRequest( SocketAddress remoteAddress, RequestType requestType, RequestContext requestContext );

    void endRequest( Throwable t );
}
