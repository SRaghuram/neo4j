/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.management;


import java.io.File;
import java.util.EnumSet;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.CoreStateFiles;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.management.CausalClustering;

@Service.Implementation( ManagementBeanProvider.class )
public class CausalClusteringBean extends ManagementBeanProvider
{
    private static final EnumSet<OperationalMode> CLUSTERING_MODES = EnumSet.of( OperationalMode.core, OperationalMode.read_replica );

    @SuppressWarnings( "WeakerAccess" ) // Bean needs public constructor
    public CausalClusteringBean()
    {
        super( CausalClustering.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management )
    {
        if ( isCausalClustering( management ) )
        {
            return new CausalClusteringBeanImpl( management, false );
        }
        return null;
    }

    @Override
    protected Neo4jMBean createMXBean( ManagementData management )
    {
        if ( isCausalClustering( management ) )
        {
            return new CausalClusteringBeanImpl( management, true );
        }
        return null;
    }

    private static boolean isCausalClustering( ManagementData management )
    {
        DatabaseInfo databaseInfo = management.resolveDependency( DatabaseInfo.class );
        return CLUSTERING_MODES.contains( databaseInfo.operationalMode );
    }

    private static class CausalClusteringBeanImpl extends Neo4jMBean implements CausalClustering
    {
        private final ClusterStateDirectory clusterStateDirectory;
        private final RaftMachine raftMachine;
        private final FileSystemAbstraction fs;

        CausalClusteringBeanImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            clusterStateDirectory = management.resolveDependency( ClusterStateDirectory.class );
            raftMachine = management.resolveDependency( RaftMachine.class );

            fs = management.getKernelData().getFilesystemAbstraction();
        }

        @Override
        public String getRole()
        {
            return raftMachine.currentRole().toString();
        }

        @Override
        public long getRaftLogSize()
        {
            File raftLogDirectory = CoreStateFiles.RAFT_LOG.at( clusterStateDirectory.get() );
            return FileUtils.size( fs, raftLogDirectory );
        }

        @Override
        public long getReplicatedStateSize()
        {
            File replicatedStateDirectory = clusterStateDirectory.get();

            File[] files = fs.listFiles( replicatedStateDirectory );
            if ( files == null )
            {
                return 0L;
            }

            long size = 0L;
            for ( File file : files )
            {
                if ( fs.isDirectory( file ) && file.getName().equals( CoreStateFiles.RAFT_LOG.directoryName() ) )
                {
                    continue;
                }

                size += FileUtils.size( fs, file );
            }

            return size;
        }
    }
}
