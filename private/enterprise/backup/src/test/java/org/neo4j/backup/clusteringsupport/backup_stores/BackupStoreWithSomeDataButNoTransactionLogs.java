/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport.backup_stores;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.io.File;

import static com.neo4j.causalclustering.helpers.DataCreator.createEmptyNodes;

public class BackupStoreWithSomeDataButNoTransactionLogs extends AbstractStoreGenerator
{
    @Override
    CoreClusterMember createData( Cluster<?> cluster ) throws Exception
    {
        return createEmptyNodes( cluster, 10 );
    }

    @Override
    void modify( File backup ) throws Exception
    {
        deleteTransactionLogs( backup );
    }
}
