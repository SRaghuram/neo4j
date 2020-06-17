/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.helper.Workload;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

enum Workloads
{
    CreateNodesWithProperties
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new CreateNodesWithProperties( control, resources, config );
                }
            },
    StartStopRandomMember
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new StartStopRandomMember( control, resources );
                }
            },
    StartStopRandomCore
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new StartStopRandomCore( control, resources );
                }
            },
    StartStopDefaultDatabaseLeader
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new StartStopLeader( control, resources, DEFAULT_DATABASE_NAME );
                }
            },
    StartStopSystemDatabaseLeader
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new StartStopLeader( control, resources, SYSTEM_DATABASE_NAME );
                }
            },
    BackupRandomMember
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new BackupRandomMember( control, resources );
                }
            },
    CatchupNewReadReplica
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new CatchupNewReadReplica( control, resources );
                }
            },
    ReplaceRandomMember
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new ReplaceRandomMember( control, resources );
                }
            },
    IdReuseInsertion
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new IdReuse.InsertionWorkload( control, resources );
                }
            },
    IdReuseDeletion
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new IdReuse.DeletionWorkload( control, resources );
                }
            },
    IdReuseReelection
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new IdReuse.ReelectionWorkload( control, resources, config );
                }
            },
    FailingWorkload
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new FailingWorkload( control );
                }
            },
    CreateManyDatabases
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new CreateManyDatabases( control, resources, config );
                }
            },
    VmPauseDuringBecomingLeader
            {
                @Override
                Workload create( Control control, Resources resources, Config config )
                {
                    return new VmPauseDuringBecomingLeader( control, resources, config );
                }
            };

    abstract Workload create( Control control, Resources resources, Config config );
}
