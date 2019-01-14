/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.backup_stores;

import java.io.File;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;

import static org.neo4j.causalclustering.helpers.DataCreator.createEmptyNodes;

public class BackupStoreWithSomeData extends AbstractStoreGenerator
{
    @Override
    CoreClusterMember createData( Cluster<?> cluster ) throws Exception
    {
        return createEmptyNodes( cluster, 15 );
    }

    @Override
    void modify( File backup )
    {
        // do nothing
    }
}
