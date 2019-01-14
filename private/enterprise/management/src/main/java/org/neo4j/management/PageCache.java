/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = PageCache.NAME )
@Description( "Information about the Neo4j page cache. " +
              "All numbers are counts and sums since the Neo4j instance was started" )
@Deprecated
public interface PageCache
{
    String NAME = "Page cache";

    @Description( "Number of page faults. How often requested data was not found in memory and had to be loaded." )
    long getFaults();

    @Description( "Number of page evictions. How many pages have been removed from memory to make room for other pages." )
    long getEvictions();

    @Description( "Number of page pins. How many pages have been accessed (monitoring must be enabled separately)." )
    long getPins();

    @Description( "Number of page flushes. How many dirty pages have been written to durable storage." )
    long getFlushes();

    @Description( "Number of bytes read from durable storage." )
    long getBytesRead();

    @Description( "Number of bytes written to durable storage." )
    long getBytesWritten();

    @Description( "Number of files that have been mapped into the page cache." )
    long getFileMappings();

    @Description( "Number of files that have been unmapped from the page cache." )
    long getFileUnmappings();

    @Description( "Number of exceptions caught during page eviction. " +
                  "This number should be zero, or at least not growing, in a healthy database. " +
                  "Otherwise it could indicate drive failure, storage space, or permission problems." )
    long getEvictionExceptions();

    @Description( "The percentage of used pages. Will return NaN if it cannot be determined." )
    default double getUsageRatio()
    {
        return Double.NaN;
    }

    @Description( "Ratio of hits to the total number of lookups in the page cache" )
    default double getHitRatio()
    {
       return Double.NaN;
    }

    @Description( "Number of page unpins. How many pages have been accessed and are not accessed anymore (monitoring must be enabled separately)." )
    default long getUnpins()
    {
        return 0;
    }

    @Description( "Number of page hits. How often requested data was found in memory." )
    default long getHits()
    {
        return 0;
    }
}
