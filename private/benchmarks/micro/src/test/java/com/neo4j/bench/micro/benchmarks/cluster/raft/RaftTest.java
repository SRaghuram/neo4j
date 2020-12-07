/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.bench.micro.benchmarks.cluster.raft.ProtocolVersion.LATEST;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RaftTest
{
    @Test
    void checkProtocolVersions()
    {
        var supported = new SupportedProtocolCreator( Config.defaults(), NullLogProvider.nullLogProvider() );
        var productionProtocols = supported.getSupportedRaftProtocolsFromConfiguration().versions().stream()
                .sorted().collect( Collectors.toList() );
        var benchmarkedProtocols = Arrays.stream( ProtocolVersion.values() ).filter( pv -> pv != LATEST )
                .map( ProtocolVersion::version ).collect( Collectors.toList() );
        assertEquals( productionProtocols, benchmarkedProtocols );
        assertEquals( LATEST.version(), productionProtocols.get( productionProtocols.size() - 1 ) );
    }

    @Test
    void serializeTxByteArray() throws Throwable
    {
        ReplicatedTxByteArray.DEBUG = true;
        var c = new ReplicatedTxByteArray()
        {
            void pre() throws Throwable
            {
                benchmarkSetup( null, null, null, null );
            }

            void post() throws Throwable
            {
                benchmarkTearDown();
            }
        };
        c.protocolVersion = LATEST;
        c.txSize = "100kb";
        c.pre();
        c.serializeContent();
        c.post();
    }

    @Test
    void serializeTxRepresentation() throws Throwable
    {
        ReplicatedTxRepresentation.DEBUG = true;
        var c = new ReplicatedTxRepresentation()
        {
            void pre() throws Throwable
            {
                benchmarkSetup( null, null, null, null );
            }

            void post() throws Throwable
            {
                benchmarkTearDown();
            }
        };
        c.protocolVersion = LATEST;
        c.txSize = "100kb";
        c.pre();
        c.serializeContent();
        c.post();
    }
}
