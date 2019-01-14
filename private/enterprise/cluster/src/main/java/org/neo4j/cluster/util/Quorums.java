/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.util;

public class Quorums
{
    private Quorums()
    {
    }

    /** Determines if a number of available members qualify as a majority, given the total number of members. */
    public static boolean isQuorum( long availableMembers, long totalMembers )
    {
        return availableMembers >= Math.floor( (totalMembers / 2) + 1 );
    }
}
