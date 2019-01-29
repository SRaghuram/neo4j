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

class LatenciesAtCoresAtSockets implements Comparable<LatenciesAtCoresAtSockets>
{
    final String name;
    final int sockets;
    final int cores;
    final long count;
    final double mean;
    final double percentile50;
    final double percentile90;
    final double percentile95;
    final double percentile99;

    LatenciesAtCoresAtSockets(
            String name,
            int sockets,
            int cores,
            long count,
            double mean,
            double percentile50,
            double percentile90,
            double percentile95,
            double percentile99 )
    {
        this.name = name;
        this.sockets = sockets;
        this.cores = cores;
        this.count = count;
        this.mean = mean;
        this.percentile50 = percentile50;
        this.percentile90 = percentile90;
        this.percentile95 = percentile95;
        this.percentile99 = percentile99;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        LatenciesAtCoresAtSockets that = (LatenciesAtCoresAtSockets) o;

        if ( cores != that.cores )
        {
            return false;
        }
        if ( sockets != that.sockets )
        {
            return false;
        }
        if ( name != null ? !name.equals( that.name ) : that.name != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + sockets;
        result = 31 * result + cores;
        return result;
    }

    @Override
    public String toString()
    {
        return "(" + sockets + "," + cores + "," + name + "," + mean + "," + count + "," + percentile50 + "," +
               percentile90 + "," + percentile95 + "," + percentile99 + ")";
    }

    @Override
    public int compareTo( LatenciesAtCoresAtSockets other )
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
                if ( other.name.compareTo( this.name ) != 0 )
                {
                    return other.name.compareTo( this.name );
                }
                else
                {
                    throw new RuntimeException(
                            format( "Multiple entries with same cores %s and sockets %s and name %s",
                                    other.cores,
                                    other.sockets,
                                    other.name
                            ) );
                }
            }
        }
    }
}
