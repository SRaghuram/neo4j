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

package com.neo4j.bench.ldbc;

public class TestUtils
{
    public static final double LOWER_PERCENT = 0.9;
    public static final double UPPER_PERCENT = 1.1;
    public static final long DIFFERENCE_ABSOLUTE = 50;

    public static long operationCountLower( long operationCount )
    {
        return Math.min(
                percent( operationCount, LOWER_PERCENT ),
                operationCount - DIFFERENCE_ABSOLUTE
        );
    }

    public static long operationCountUpper( long operationCount )
    {
        return Math.max(
                percent( operationCount, UPPER_PERCENT ),
                operationCount + DIFFERENCE_ABSOLUTE
        );
    }

    public static long percent( long value, double percent )
    {
        return Math.round( value * percent );
    }
}
