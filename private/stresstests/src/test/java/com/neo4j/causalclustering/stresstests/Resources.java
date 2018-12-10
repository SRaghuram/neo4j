/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DefaultCluster;
import com.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyMap;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;

class Resources
{
    private final Cluster<?> cluster;
    private final File clusterDir;
    private final File backupDir;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final LogProvider logProvider;

    Resources( FileSystemAbstraction fileSystem, PageCache pageCache, Config config ) throws IOException
    {
        this( fileSystem, pageCache, FormattedLogProvider.toOutputStream( System.out ), config );
    }

    private Resources( FileSystemAbstraction fileSystem, PageCache pageCache, LogProvider logProvider, Config config ) throws IOException
    {
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.logProvider = logProvider;

        int numberOfCores = config.numberOfCores();
        int numberOfEdges = config.numberOfEdges();
        String workingDirectory = config.workingDir();

        this.clusterDir = ensureExistsAndEmpty( new File( workingDirectory, "cluster" ) );
        this.backupDir = ensureExistsAndEmpty( new File( workingDirectory, "backups" ) );

        Map<String,String> coreParams = new HashMap<>();
        Map<String,String> readReplicaParams = new HashMap<>();

        config.populateCoreParams( coreParams );
        config.populateReadReplicaParams( readReplicaParams );

        HazelcastDiscoveryServiceFactory discoveryServiceFactory = new HazelcastDiscoveryServiceFactory();
        cluster = new DefaultCluster( clusterDir, numberOfCores, numberOfEdges, discoveryServiceFactory, coreParams, emptyMap(), readReplicaParams,
                emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
    }

    public Cluster<?> cluster()
    {
        return cluster;
    }

    public FileSystemAbstraction fileSystem()
    {
        return fileSystem;
    }

    public LogProvider logProvider()
    {
        return logProvider;
    }

    public File backupDir()
    {
        return backupDir;
    }

    public PageCache pageCache()
    {
        return pageCache;
    }

    public void start() throws Exception
    {
        cluster.start();
    }

    public void stop()
    {
        cluster.shutdown();
    }

    public void cleanup() throws IOException
    {
        FileUtils.deleteRecursively( clusterDir );
        FileUtils.deleteRecursively( backupDir );
    }

    public Clock clock()
    {
        return Clock.systemUTC();
    }
}
