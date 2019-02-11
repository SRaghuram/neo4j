/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.ClusterMember;

import java.io.File;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.stresstests.ConsistencyHelper.assertStoreConsistent;

public class StartStopMember implements WorkOnMember
{
    private final Log log;
    private final FileSystemAbstraction fileSystem;
    private final File tempStoreDir;

    StartStopMember( Resources resources )
    {
        this.log = resources.logProvider().getLog( getClass() );
        this.fileSystem = resources.fileSystem();
        this.tempStoreDir = resources.tempStoreDir();
    }

    @Override
    public void doWorkOnMember( ClusterMember member ) throws Exception
    {
        DatabaseLayout databaseLayout = member.database().databaseLayout();
        log.info( "Stopping: " + member );
        member.shutdown();

        assertStoreConsistent( fileSystem, tempStoreDir, databaseLayout );

        Thread.sleep( 5000 );
        log.info( "Starting: " + member );
        member.start();
    }
}
