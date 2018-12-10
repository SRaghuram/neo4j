/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import java.io.File;

import com.neo4j.causalclustering.common.ClusterMember;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.stresstests.ConsistencyHelper.assertStoreConsistent;

public class StartStopMember implements WorkOnMember
{
    private final Log log;
    private final FileSystemAbstraction fileSystem;

    StartStopMember( Resources resources )
    {
        this.log = resources.logProvider().getLog( getClass() );
        this.fileSystem = resources.fileSystem();
    }

    @Override
    public void doWorkOnMember( ClusterMember member ) throws Exception
    {
        File databaseDirectory = member.database().databaseLayout().databaseDirectory();
        log.info( "Stopping: " + member );
        member.shutdown();

        assertStoreConsistent( fileSystem, databaseDirectory );

        Thread.sleep( 5000 );
        log.info( "Starting: " + member );
        member.start();
    }
}
