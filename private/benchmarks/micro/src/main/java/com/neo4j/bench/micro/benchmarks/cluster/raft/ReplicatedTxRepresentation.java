/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.bench.micro.benchmarks.cluster.ClusterTx;
import com.neo4j.bench.micro.benchmarks.cluster.ProtocolVersion;
import com.neo4j.bench.micro.benchmarks.cluster.TxFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import java.util.concurrent.ExecutionException;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.logging.Log;

import static com.neo4j.bench.micro.Main.run;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class ReplicatedTxRepresentation extends AbstractRaftBenchmark
{
    @ParamValues( allowed = {"V2"}, base = "V2" )
    @Param( {} )
    public ProtocolVersion protocolVersion;

    @ParamValues( allowed = {"1KB", "1MB", "100MB", "1GB"}, base = {"1KB", "1MB", "100MB", "1GB"} )
    @Param( {} )
    public String txSize;

    @Override
    public String description()
    {
        return "Raft replicated transaction transfer using transaction representation";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    ProtocolVersion protocolVersion()
    {
        return protocolVersion;
    }

    @Override
    RaftMessages.ClusterIdAwareMessage<RaftMessages.RaftMessage> initializeRaftMessage()
    {
        int expectedSize = nbrOfBytes( txSize );
        TxFactory.commitTx( expectedSize, db() );
        ClusterTx clusterTx = popLatest();
        Log log = logProvider().getLog( getClass() );
        log.info( "Created transaction representation of size: %d. Expected: %d. Diff%%: %f", clusterTx.size(), expectedSize,
                diffPercent( expectedSize, clusterTx.size() ) );
        return RaftMessages.ClusterIdAwareMessage.of( CLUSTER_ID,
                new RaftMessages.NewEntry.Request( MEMBER_ID, ReplicatedTransaction.from( clusterTx.txRepresentation(), DEFAULT_DATABASE_NAME ) ) );
    }

    private float diffPercent( int expectedSize, int size )
    {
        return Math.abs( 1 - size / (float) expectedSize );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void serializeContent() throws InterruptedException, ExecutionException
    {
        sendOneWay();
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReplicatedTxRepresentation.class, methods );
    }
}
