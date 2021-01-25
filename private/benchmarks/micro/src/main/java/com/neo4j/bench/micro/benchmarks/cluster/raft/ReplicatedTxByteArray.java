/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.bench.jmh.api.config.BenchmarkEnabled;
import com.neo4j.bench.jmh.api.config.ParamValues;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import com.neo4j.causalclustering.identity.RaftGroupId;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;

import java.util.concurrent.ExecutionException;

import static com.neo4j.bench.micro.Main.run;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

@BenchmarkEnabled( true )
@OutputTimeUnit( MICROSECONDS )
public class ReplicatedTxByteArray extends AbstractRaftBenchmark
{
    @ParamValues(
            allowed = {"V2", "V3", "LATEST"},
            base = {"V2", "V3", "LATEST"} )
    @Param( {} )
    public ProtocolVersion protocolVersion;

    @ParamValues(
            allowed = {"1KB", "1MB", "100MB", "999MB"},
            base = {"1KB", "1MB", "100MB", "999MB"} )
    @Param( {} )
    public String txSize;

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
        return protocolVersion;
    }

    @Override
    RaftMessages.OutboundRaftMessageContainer<RaftMessages.RaftMessage> initializeRaftMessage()
    {
        byte[] bytes = new byte[nbrOfBytes( txSize )];
        return RaftMessages.OutboundRaftMessageContainer.of(
                RaftGroupId.from( DATABASE_ID ),
                new RaftMessages.NewEntry.Request( AbstractRaftBenchmark.MEMBER_ID,
                                                   ReplicatedTransaction.from( bytes, DATABASE_ID ) ) );
    }

    @Benchmark
    @BenchmarkMode( Mode.SampleTime )
    public void serializeContent() throws InterruptedException, ExecutionException
    {
        sendOneWay();
    }

    public static void main( String... methods )
    {
        run( ReplicatedTxByteArray.class, methods );
    }
}
