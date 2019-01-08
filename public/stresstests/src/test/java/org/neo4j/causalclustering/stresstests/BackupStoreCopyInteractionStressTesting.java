/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.stresstests;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.lang.System.getProperty;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertNull;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.configureBackup;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.configureRaftLogRotationAndPruning;
import static org.neo4j.causalclustering.stresstests.ClusterConfiguration.enableRaftMessageLogging;
import static org.neo4j.function.Suppliers.untilTimeExpired;
import static org.neo4j.helper.DatabaseConfiguration.configureTxLogRotationAndPruning;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.helper.StressTestingHelper.fromEnv;

public class BackupStoreCopyInteractionStressTesting
{
    private static final String DEFAULT_NUMBER_OF_CORES = "3";
    private static final String DEFAULT_NUMBER_OF_EDGES = "1";
    private static final String DEFAULT_DURATION_IN_MINUTES = "30";
    private static final String DEFAULT_ENABLE_INDEXES = "false";
    private static final String DEFAULT_TX_PRUNE = "50 files";
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();
    private static final String DEFAULT_BASE_CORE_BACKUP_PORT = "8000";
    private static final String DEFAULT_BASE_EDGE_BACKUP_PORT = "9000";

    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule );

    private FileSystemAbstraction fs;
    private PageCache pageCache;

    @Before
    public void setUp()
    {
        fs = fileSystemRule.get();
        pageCache = pageCacheRule.getPageCache( fs );
    }

    @Test
    public void shouldBehaveCorrectlyUnderStress() throws Exception
    {
        int numberOfCores =
                parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_NUMBER_OF_CORES", DEFAULT_NUMBER_OF_CORES ) );
        int numberOfEdges =
                parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_NUMBER_OF_EDGES", DEFAULT_NUMBER_OF_EDGES ) );
        long durationInMinutes =
                parseLong( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_DURATION", DEFAULT_DURATION_IN_MINUTES ) );
        String workingDirectory =
                fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        int baseCoreBackupPort = parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_BASE_CORE_BACKUP_PORT",
                DEFAULT_BASE_CORE_BACKUP_PORT ) );
        int baseEdgeBackupPort = parseInt( fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_BASE_EDGE_BACKUP_PORT",
                DEFAULT_BASE_EDGE_BACKUP_PORT ) );
        boolean enableIndexes = parseBoolean(
                fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_ENABLE_INDEXES", DEFAULT_ENABLE_INDEXES ) );
        String txPrune = fromEnv( "BACKUP_STORE_COPY_INTERACTION_STRESS_TX_PRUNE", DEFAULT_TX_PRUNE );

        File clusterDirectory = ensureExistsAndEmpty( new File( workingDirectory, "cluster" ) );
        File backupDirectory = ensureExistsAndEmpty( new File( workingDirectory, "backups" ) );

        BiFunction<Boolean,Integer,SocketAddress> backupAddress = ( isCore, id ) ->
                new AdvertisedSocketAddress( "localhost", (isCore ? baseCoreBackupPort : baseEdgeBackupPort) + id );

        Map<String,String> coreParams = enableRaftMessageLogging(
                configureRaftLogRotationAndPruning( configureTxLogRotationAndPruning( new HashMap<>(), txPrune ) ) );
        Map<String,String> readReplicaParams = configureTxLogRotationAndPruning( new HashMap<>(), txPrune );

        Map<String,IntFunction<String>> instanceCoreParams =
                configureBackup( new HashMap<>(), id -> backupAddress.apply( true, id ) );
        Map<String,IntFunction<String>> instanceReadReplicaParams =
                configureBackup( new HashMap<>(), id -> backupAddress.apply( false, id ) );

        HazelcastDiscoveryServiceFactory discoveryServiceFactory = new HazelcastDiscoveryServiceFactory();
        Cluster cluster =
                new Cluster( clusterDirectory, numberOfCores, numberOfEdges, discoveryServiceFactory, coreParams,
                        instanceCoreParams, readReplicaParams, instanceReadReplicaParams, Standard.LATEST_NAME,
                        IpFamily.IPV4, false );

        AtomicBoolean stopTheWorld = new AtomicBoolean();
        BooleanSupplier notExpired = untilTimeExpired( durationInMinutes, MINUTES );
        BooleanSupplier keepGoing = () -> !stopTheWorld.get() && notExpired.getAsBoolean();
        Runnable onFailure = () -> stopTheWorld.set( true );

        ExecutorService service = Executors.newFixedThreadPool( 3 );
        try
        {
            cluster.start();
            if ( enableIndexes )
            {
                Workload.setupIndexes( cluster );
            }

            Future<Throwable> workload = service.submit( new Workload( keepGoing, onFailure, cluster ) );
            Future<Throwable> startStopWorker = service.submit(
                    new StartStopLoad( fs, pageCache, keepGoing, onFailure, cluster, numberOfCores, numberOfEdges ) );
            Future<Throwable> backupWorker = service.submit(
                    new BackupLoad( keepGoing, onFailure, cluster, numberOfCores, numberOfEdges, backupDirectory,
                            backupAddress ) );

            long timeout = durationInMinutes + 5;
            assertNull( Exceptions.stringify( workload.get() ), workload.get( timeout, MINUTES ) );
            assertNull( Exceptions.stringify( startStopWorker.get() ), startStopWorker.get( timeout, MINUTES ) );
            assertNull( Exceptions.stringify( backupWorker.get() ), backupWorker.get( timeout, MINUTES ) );
        }
        finally
        {
            cluster.shutdown();
            service.shutdown();
        }

        // let's cleanup disk space when everything went well
        FileUtils.deleteRecursively( clusterDirectory );
        FileUtils.deleteRecursively( backupDirectory );
    }
}
