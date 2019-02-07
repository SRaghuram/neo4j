/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.bench.micro.benchmarks.cluster.ProtocolVersion;
import com.neo4j.bench.micro.config.BenchmarkEnabled;
import com.neo4j.bench.micro.config.ParamValues;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import java.util.concurrent.ExecutionException;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;

import static com.neo4j.bench.micro.Main.run;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class ReplicatedTxByteArray extends AbstractRaftBenchmark
{
    @ParamValues( allowed = {"V1", "V2"}, base = "V2" )
    @Param( {} )
    public ProtocolVersion ReplicatedTxByteArray_protocolVersion;

    @ParamValues( allowed = {"1KB", "1MB", "100MB", "999MB"}, base = {"1KB", "1MB", "100MB", "999MB"} )
    @Param( {} )
    public String ReplicatedTxByteArray_txSize;

    @Override
    public String description()
    {
        return "Raft replicated transaction transfer using byte array representation";
    }

    @Override
    public boolean isThreadSafe()
    {
        return false;
    }

    @Override
    ProtocolVersion protocolVersion()
    {
        return ReplicatedTxByteArray_protocolVersion;
    }

    @Override
    RaftMessages.ClusterIdAwareMessage<RaftMessages.RaftMessage> initializeRaftMessage()
    {
        return RaftMessages.ClusterIdAwareMessage.of( AbstractRaftBenchmark.CLUSTER_ID, new RaftMessages.NewEntry.Request( AbstractRaftBenchmark.MEMBER_ID,
                ReplicatedTransaction.from( new byte[nbrOfBytes( ReplicatedTxByteArray_txSize )] ) ) );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void serializeContent() throws InterruptedException, ExecutionException
    {
        sendOneWay();
    }

    public static void main( String... methods ) throws Exception
    {
        run( ReplicatedTxByteArray.class, methods );
    }
}
