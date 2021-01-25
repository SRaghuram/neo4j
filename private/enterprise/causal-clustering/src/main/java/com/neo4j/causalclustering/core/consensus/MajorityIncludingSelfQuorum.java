/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import java.util.Collection;

public class MajorityIncludingSelfQuorum
{
    private static final int MIN_QUORUM = 2;

    private MajorityIncludingSelfQuorum()
    {
    }

    public static boolean isQuorum( Collection<?> cluster, Collection<?> countNotIncludingMyself )
    {
        return isQuorum( cluster.size(), countNotIncludingMyself.size() );
    }

    public static boolean isQuorum( int clusterSize, int countNotIncludingSelf )
    {
        return isQuorum( MIN_QUORUM, clusterSize, countNotIncludingSelf );
    }

    public static boolean isQuorum( int minQuorum, int clusterSize, int countNotIncludingSelf )
    {
        return (countNotIncludingSelf + 1) >= minQuorum &&
                countNotIncludingSelf >= clusterSize / 2;
    }
}
