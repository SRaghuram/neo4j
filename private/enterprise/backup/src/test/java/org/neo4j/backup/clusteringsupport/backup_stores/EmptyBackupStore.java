/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport.backup_stores;

import java.io.File;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

public class EmptyBackupStore extends AbstractStoreGenerator
{
    @Override
    CoreClusterMember createData( Cluster<?> cluster )
    {
        return cluster.coreMembers().iterator().next();
    }

    @Override
    void modify( File backup ) throws Exception
    {
        // do nothing
    }
}
