/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
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
