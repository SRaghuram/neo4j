/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = RemoteConnection.NAME )
@Deprecated
public interface RemoteConnection
{
    String NAME = "Remote Connection";
}
