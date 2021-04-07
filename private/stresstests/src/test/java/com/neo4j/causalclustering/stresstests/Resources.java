/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.discovery.DiscoveryServiceType;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.driver.DriverFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.Driver;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;

import static com.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;

class Resources
{
    private final Cluster cluster;
    private final Driver driver;
    private final Path clusterDir;
    private final Path backupDir;
    private final FileSystemAbstraction fileSystem;
    private final PageCache pageCache;
    private final LogProvider logProvider;
    private final AtomicLong nodeIdCounter = new AtomicLong();

    Resources( FileSystemAbstraction fileSystem, PageCache pageCache, Config config, DriverFactory driverFactory ) throws IOException
    {
        this.fileSystem = fileSystem;
        this.pageCache = pageCache;
        this.logProvider = new Log4jLogProvider( System.out );

        var numberOfCores = config.numberOfCores();
        var numberOfReadReplicas = config.numberOfEdges();
        var workingDirectory = config.workingDir();

        this.clusterDir = ensureExistsAndEmpty( Path.of( workingDirectory, "cluster" ) );
        this.backupDir = ensureExistsAndEmpty( Path.of( workingDirectory, "backups" ) );

        var coreParams = new HashMap<String,String>();
        var readReplicaParams = new HashMap<String,String>();

        config.populateCoreParams( coreParams );
        config.populateReadReplicaParams( readReplicaParams );

        ClusterConfig clusterConfig = ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( numberOfCores )
                .withNumberOfReadReplicas( numberOfReadReplicas )
                .withDiscoveryServiceType( DiscoveryServiceType.AKKA )
                .withSharedPrimaryParams( coreParams )
                .withSharedReadReplicaParams( readReplicaParams )
                .withRecordFormat( Standard.LATEST_NAME )
                .withIpFamily( IpFamily.IPV4 )
                .useWildcard( false );
        cluster = clusterConfig.build( clusterDir );
        driver = driverFactory.graphDatabaseDriver( cluster );
    }

    public Cluster cluster()
    {
        return cluster;
    }

    public Driver driver()
    {
        return driver;
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
        FileUtils.deleteDirectory( clusterDir );
        FileUtils.deleteDirectory( backupDir );
    }

    public Clock clock()
    {
        return Clock.systemUTC();
    }

    public long getNextNodeIndex()
    {
        return nodeIdCounter.getAndIncrement();
    }
}
