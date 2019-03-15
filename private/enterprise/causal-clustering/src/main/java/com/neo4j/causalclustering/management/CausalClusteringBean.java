/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.management;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;

import java.io.File;
import java.util.EnumSet;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.management.CausalClustering;

import static com.neo4j.causalclustering.core.state.CoreStateFiles.RAFT_LOG;

@ServiceProvider
public class CausalClusteringBean extends ManagementBeanProvider
{
    private static final EnumSet<OperationalMode> CLUSTERING_MODES = EnumSet.of( OperationalMode.CORE, OperationalMode.READ_REPLICA );

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
        private final String databaseName;
        private final ClusterStateLayout clusterStateLayout;
        private final RaftMachine raftMachine;
        private final FileSystemAbstraction fs;

        CausalClusteringBeanImpl( ManagementData management, boolean isMXBean )
        {
            super( management, isMXBean );
            databaseName = management.getDatabase().getDatabaseName();
            clusterStateLayout = management.resolveDependency( ClusterStateLayout.class );
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
            File raftLogDirectory = clusterStateLayout.raftLogDirectory( databaseName );
            return FileSystemUtils.size( fs, raftLogDirectory );
        }

        @Override
        public long getReplicatedStateSize()
        {
            return clusterStateLayout.listGlobalAndDatabaseDirectories( databaseName, type -> type != RAFT_LOG )
                    .stream()
                    .mapToLong( dir -> FileSystemUtils.size( fs, dir ) )
                    .sum();
        }
    }
}
