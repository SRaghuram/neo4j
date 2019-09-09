/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.NullLogProvider;

/**
 * Check the consistency of all the cluster members' stores.
 */
public class ConsistencyCheck extends Validation
{
    private final Cluster cluster;

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
            DatabaseLayout databaseLayout = member.databaseLayout();
            ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck( databaseLayout, Config.defaults(),
                    ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true );
            if ( !result.isSuccessful() )
            {
                throw new RuntimeException( "Not consistent database in " + databaseLayout );
            }
        }
    }

    @Override
    protected boolean postStop()
    {
        return true;
    }
}
