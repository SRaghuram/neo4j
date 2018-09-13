/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import org.neo4j.causalclustering.common.ClusterMember;

interface WorkOnMember
{
    void doWorkOnMember( ClusterMember member ) throws Exception;
}
