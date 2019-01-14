/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@Deprecated
@ManagementInterface( name = MemoryMapping.NAME )
@Description( "The status of Neo4j memory mapping" )
public interface MemoryMapping
{
    String NAME = "Memory Mapping";

    @Deprecated
    @Description( "Get information about each pool of memory mapped regions from store files with "
                  + "memory mapping enabled" )
    WindowPoolInfo[] getMemoryPools();
}
