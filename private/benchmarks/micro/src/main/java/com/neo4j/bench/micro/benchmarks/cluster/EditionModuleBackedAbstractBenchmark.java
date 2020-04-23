/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import com.neo4j.bench.common.results.ForkDirectory;
import com.neo4j.bench.micro.benchmarks.BaseRegularBenchmark;
import com.neo4j.bench.model.model.Benchmark;
import com.neo4j.bench.model.model.BenchmarkGroup;
import com.neo4j.bench.model.model.Neo4jConfig;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionMarshalV2;
import com.neo4j.causalclustering.core.state.machines.tx.TransactionRepresentationReplicatedTransaction;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Stack;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.api.security.provider.NoAuthSecurityProvider;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.noOpSystemGraphInitializer;

public abstract class EditionModuleBackedAbstractBenchmark extends BaseRegularBenchmark
{
    private final Stack<ClusterTx> clusterTxStack = new Stack<>();
    private GraphDatabaseFacade graphDatabaseFacade;
    private DatabaseManagementService managementService;
    private GlobalModule globalModule;
    private Config config;
    protected static final NamedDatabaseId NAMED_DATABASE_ID = TestDatabaseIdRepository.randomNamedDatabaseId();
    protected static final DatabaseId DATABASE_ID = NAMED_DATABASE_ID.databaseId();

    protected GraphDatabaseFacade db()
    {
        return graphDatabaseFacade;
    }

    protected GlobalModule module()
    {
        return globalModule;
    }

    public Config config()
    {
        return config;
    }

    protected abstract void setUp() throws Throwable;

    protected abstract void shutdown();

    protected AbstractEditionModule createModule( GlobalModule globalModule )
    {
        return new TxProbingEditionModule( globalModule );
    }

    @Override
    protected void benchmarkSetup( BenchmarkGroup group,
                                   Benchmark benchmark,
                                   Neo4jConfig neo4jConfig,
                                   ForkDirectory forkDirectory ) throws Throwable
    {
        var tempDirectory = forkDirectory == null ? Path.of( "." ) : Paths.get( forkDirectory.toAbsolutePath() );
        tempDirectory = tempDirectory.resolve( "temp-db" ).toAbsolutePath().normalize();
        FileUtils.deleteDirectory( tempDirectory.toFile() );
        config = Config.newBuilder().set( GraphDatabaseSettings.neo4j_home, tempDirectory ).build();
        managementService = new DatabaseManagementServiceFactory( DatabaseInfo.COMMUNITY, this::saveModule )
                .build( config, GraphDatabaseDependencies.newDependencies().dependencies( noOpSystemGraphInitializer() ) );
        graphDatabaseFacade = (GraphDatabaseFacade) managementService.database( Config.defaults().get( GraphDatabaseSettings.default_database ) );
        setUp();
    }

    @Override
    protected void benchmarkTearDown() throws Throwable
    {
        shutdown();
        managementService.shutdown();
        FileUtils.deleteDirectory( config.get( GraphDatabaseSettings.neo4j_home ).toFile() );
    }

    protected ClusterTx popLatest()
    {
        return clusterTxStack.pop();
    }

    private AbstractEditionModule saveModule( GlobalModule  globalModule )
    {
        this.globalModule = globalModule;
        return createModule( globalModule );
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
                    TransactionRepresentationReplicatedTransaction txRepresentation =
                            ReplicatedTransaction.from( batch.transactionRepresentation(), NAMED_DATABASE_ID );

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
