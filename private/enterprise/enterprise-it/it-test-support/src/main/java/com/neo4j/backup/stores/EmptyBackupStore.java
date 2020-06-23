/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.nio.file.Path;

public class EmptyBackupStore extends AbstractStoreGenerator
{
    @Override
    CoreClusterMember createData( Cluster cluster )
    {
        return cluster.coreMembers().iterator().next();
    }

    @Override
    void modify( Path backup ) throws Exception
    {
        // do nothing
    }
}
