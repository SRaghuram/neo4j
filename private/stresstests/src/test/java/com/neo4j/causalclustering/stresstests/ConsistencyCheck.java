/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLogProvider;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;

/**
 * Check the consistency of all the cluster members' stores.
 */
public class ConsistencyCheck extends Validation
{
    private final Cluster cluster;
    private final FileSystemAbstraction fs;
    private final Log log;

    ConsistencyCheck( Resources resources )
    {
        super();
        cluster = resources.cluster();
        fs = resources.fileSystem();
        log = resources.logProvider().getLog( getClass() );
    }

    @Override
    protected void validate() throws Exception
    {
        Iterable<ClusterMember> members = Iterables.concat( cluster.coreMembers(), cluster.readReplicas() );

        for ( ClusterMember member : members )
        {
            Neo4jLayout neo4jLayout = member.neo4jLayout();
            for ( String databaseName : getDatabaseNamesFromFileSystem( neo4jLayout ) )
            {
                DatabaseLayout databaseLayout = neo4jLayout.databaseLayout( databaseName );
                log.info( "Checking consistency of: " + databaseLayout.databaseDirectory() );

                ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck( databaseLayout, Config.defaults(),
                        ProgressMonitorFactory.NONE, NullLogProvider.getInstance(), true );
                if ( !result.isSuccessful() )
                {
                    throw new RuntimeException( "Not consistent database in " + databaseLayout );
                }
            }
        }
    }

    private Set<String> getDatabaseNamesFromFileSystem( Neo4jLayout neo4jLayout )
    {
        return stream( fs.listFiles( neo4jLayout.databasesDirectory() ) ).filter( Files::isDirectory ).map( Path::getFileName ).map( Path::toString )
                                                                         .collect( toSet() );
    }

    @Override
    protected boolean postStop()
    {
        return true;
    }
}
