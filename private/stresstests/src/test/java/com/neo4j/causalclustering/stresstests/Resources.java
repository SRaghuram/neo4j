/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;

import static com.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static java.util.Collections.emptyMap;

class Resources
{
    private final Cluster cluster;
    private final Path clusterDir;
    private final Path backupDir;
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

        this.clusterDir = ensureExistsAndEmpty( Path.of( workingDirectory, "cluster" ) );
        this.backupDir = ensureExistsAndEmpty( Path.of( workingDirectory, "backups" ) );

        Map<String,String> coreParams = new HashMap<>();
        Map<String,String> readReplicaParams = new HashMap<>();

        config.populateCoreParams( coreParams );
        config.populateReadReplicaParams( readReplicaParams );

        DiscoveryServiceFactory discoveryServiceFactory = new AkkaDiscoveryServiceFactory();
        cluster = new Cluster( clusterDir, numberOfCores, numberOfEdges, discoveryServiceFactory, coreParams, emptyMap(), readReplicaParams,
                emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
    }

    public Cluster cluster()
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

    public Path backupDir()
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
        FileUtils.deletePathRecursively( clusterDir );
        FileUtils.deletePathRecursively( backupDir );
    }

    public Clock clock()
    {
        return Clock.systemUTC();
    }
}
