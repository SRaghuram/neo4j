/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.management;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.internal.Version;
import org.neo4j.management.ClusterDatabaseInfo;
import org.neo4j.management.ClusterMemberInfo;

import static org.neo4j.helpers.collection.Iterables.asArray;
import static org.neo4j.helpers.collection.Iterables.map;

public class HighlyAvailableKernelData extends KernelData
{
    private final ClusterMembers memberInfo;
    private final ClusterDatabaseInfoProvider memberInfoProvider;

    public HighlyAvailableKernelData( DataSourceManager dataSourceManager, ClusterMembers memberInfo,
            ClusterDatabaseInfoProvider databaseInfo, FileSystemAbstraction fileSystem, PageCache pageCache,
            File storeDir, Config config )
    {
        super( fileSystem, pageCache, storeDir, config, dataSourceManager );
        this.memberInfo = memberInfo;
        this.memberInfoProvider = databaseInfo;
    }

    @Override
    public Version version()
    {
        return Version.getKernel();
    }

    ClusterMemberInfo[] getClusterInfo()
    {
        List<ClusterMemberInfo> clusterMemberInfos = new ArrayList<>();
        Function<Object,String> nullSafeToString = from -> from == null ? "" : from.toString();
        for ( ClusterMember clusterMember : memberInfo.getMembers() )
        {
            ClusterMemberInfo clusterMemberInfo = new ClusterMemberInfo( clusterMember.getInstanceId().toString(),
                    clusterMember.getHAUri() != null, clusterMember.isAlive(), clusterMember.getHARole(),
                    asArray( String.class, map( nullSafeToString, clusterMember.getRoleURIs() ) ),
                    asArray( String.class, map( nullSafeToString, clusterMember.getRoles() ) ) );
            clusterMemberInfos.add( clusterMemberInfo );
        }

        return clusterMemberInfos.toArray( new ClusterMemberInfo[clusterMemberInfos.size()] );
    }

    ClusterDatabaseInfo getMemberInfo()
    {
        return memberInfoProvider.getInfo();
    }
}
