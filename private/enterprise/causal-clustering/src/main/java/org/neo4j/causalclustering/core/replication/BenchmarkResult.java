/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.replication;

@SuppressWarnings( "WeakerAccess" )
public class BenchmarkResult
{
    public Long totalRequests;
    public Long totalBytes;
    public Long timeMillis;
    public Double opsPerMilli;
    public Double mbPerSecond;

    BenchmarkResult( long totalRequests, long totalBytes, long totalTimeMillis )
    {
        this.totalRequests = totalRequests;
        this.totalBytes = totalBytes;
        this.timeMillis = totalTimeMillis;
        this.opsPerMilli = totalRequests / (double) totalTimeMillis;
        this.mbPerSecond = totalBytes / (double) totalTimeMillis / 1048576 * 1000;
    }
}
