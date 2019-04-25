/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.io.File;

import static com.neo4j.causalclustering.common.DataCreator.createEmptyNodes;

public class BackupStoreWithSomeData extends AbstractStoreGenerator
{
    @Override
    CoreClusterMember createData( Cluster cluster ) throws Exception
    {
        return createEmptyNodes( cluster, 15 );
    }

    @Override
    void modify( File backup ) throws Exception
    {
        // do nothing
    }
}
