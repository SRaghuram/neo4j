/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc.result_curation;

import static java.lang.String.format;

class ThroughputAtCoresAtSockets implements Comparable<ThroughputAtCoresAtSockets>
{
    final int sockets;
    final int cores;
    final double throughput;

    ThroughputAtCoresAtSockets( int sockets, int cores, double throughput )
    {
        this.sockets = sockets;
        this.cores = cores;
        this.throughput = throughput;
    }

    @Override
    public String toString()
    {
        return "(" + sockets + "," + cores + "," + throughput + ")";
    }

    @Override
    public int compareTo( ThroughputAtCoresAtSockets other )
    {
        if ( other.sockets < this.sockets )
        {
            return 1;
        }
        else if ( other.sockets > this.sockets )
        {
            return -1;
        }
        else
        {
            if ( other.cores < this.cores )
            {
                return 1;
            }
            else if ( other.cores > this.cores )
            {
                return -1;
            }
            else
            {
                throw new RuntimeException(
                        format( "Multiple entries with same cores %s and sockets %s",
                                other.cores,
                                other.sockets
                        ) );
            }
        }
    }
}
