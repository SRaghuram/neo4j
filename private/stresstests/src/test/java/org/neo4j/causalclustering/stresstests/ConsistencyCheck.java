/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.stresstests;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.helpers.collection.Iterables;

import static org.neo4j.consistency.ConsistencyCheckTool.runConsistencyCheckTool;

/**
 * Check the consistency of all the cluster members' stores.
 */
public class ConsistencyCheck extends Validation
{
    private final Cluster<?> cluster;

    ConsistencyCheck( Resources resources )
    {
        super();
        cluster = resources.cluster();
    }

    @Override
    protected void validate() throws Exception
    {
        Iterable<ClusterMember> members = Iterables.concat( cluster.coreMembers(), cluster.readReplicas() );

        for ( ClusterMember member : members )
        {
            String databasePath = member.databaseDirectory().getAbsolutePath();
            ConsistencyCheckService.Result result = runConsistencyCheckTool( new String[]{databasePath}, System.out, System.err );
            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + databasePath );
            }
        }
    }
}
