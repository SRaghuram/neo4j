/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import com.neo4j.bench.client.model.Benchmark;
import com.neo4j.bench.client.model.BenchmarkGroup;
import com.neo4j.bench.client.model.Neo4jConfig;
import com.neo4j.bench.client.profiling.FullBenchmarkName;
import com.neo4j.bench.client.util.BenchmarkUtil;
import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;
import com.neo4j.bench.micro.data.Stores;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionMarshalV2;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Stack;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public abstract class EditionModuleBackedAbstractBenchmark extends BaseRegularBenchmark
{
    private final Stack<ClusterTx> clusterTxStack = new Stack<>();
    private GraphDatabaseFacade graphDatabaseFacade;
    private Path tempDirectory;

    protected GraphDatabaseService db()
    {
        return graphDatabaseFacade;
    }

    protected abstract void setUp() throws Throwable;

    protected abstract void shutdown();

    @Override
    protected void benchmarkSetup( BenchmarkGroup group, Benchmark benchmark, Stores stores, Neo4jConfig neo4jConfig ) throws Throwable
    {
        tempDirectory = createTempDirectory( group, benchmark, stores );
        graphDatabaseFacade = (GraphDatabaseFacade) new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, TxProbingEditionModule::new ).newFacade(
                tempDirectory.toFile(), Config.defaults(), GraphDatabaseDependencies.newDependencies() ).database(
                Config.defaults().get( GraphDatabaseSettings.default_database ) );

        setUp();
    }

    private Path createTempDirectory( BenchmarkGroup group, Benchmark benchmark, Stores stores )
    {
        FullBenchmarkName benchmarkName = FullBenchmarkName.from( group, benchmark );
        String tempDirName = benchmarkName.sanitizedName() + "-temp-work-dir";
        tempDirectory = stores.storesDir().resolve( tempDirName );
        BenchmarkUtil.assertDoesNotExist( tempDirectory );
        return BenchmarkUtil.tryMkDir( tempDirectory );
    }

    @Override
    protected void benchmarkTearDown()
    {
        shutdown();
        graphDatabaseFacade.shutdown();
        BenchmarkUtil.assertDirectoryExists( tempDirectory );
        BenchmarkUtil.deleteDir( tempDirectory );
    }

    protected ClusterTx popLatest()
    {
        return clusterTxStack.pop();
    }

    class TxProbingEditionModule extends CommunityEditionModule
    {
        TxProbingEditionModule( GlobalModule globalModule )
        {
            super( globalModule );
            commitProcessFactory = ( appender, storageEngine, config ) -> (TransactionCommitProcess) ( batch, commitEvent, mode ) ->
            {
                long txId = batch.transactionId();

                CountingChannel countingChannel = new CountingChannel();
                try
                {
                    TransactionRepresentationReplicatedTransaction txRepresentation = ReplicatedTransaction.from( batch.transactionRepresentation(),
                                                                                                                  new DatabaseId( "db-name" ) );

                    ReplicatedTransactionMarshalV2.marshal( countingChannel, txRepresentation );

                    clusterTxStack.add( new ClusterTx( txId, batch.transactionRepresentation(), countingChannel.totalSize ) );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                return txId;
            };
        }

        @Override
        public void createSecurityModule( GlobalModule globalModule )
        {
            securityProvider = NoAuthSecurityProvider.INSTANCE;
        }
    }
}
