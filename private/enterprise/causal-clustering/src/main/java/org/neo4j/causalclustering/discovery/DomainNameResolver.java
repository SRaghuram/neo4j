/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import java.net.InetAddress;

public interface DomainNameResolver
{
    InetAddress[] resolveDomainName( String hostname ) throws UnknownHostException;
}
