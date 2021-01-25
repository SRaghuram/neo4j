/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.causalclustering.core.SupportedProtocolCreator;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static com.neo4j.bench.micro.benchmarks.cluster.catchup.ProtocolVersion.LATEST;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CatchupTest
{
    @Test
    void checkProtocolVersions()
    {
        var supported = new SupportedProtocolCreator( Config.defaults(), NullLogProvider.nullLogProvider() );
        var productionProtocols = supported.getSupportedCatchupProtocolsFromConfiguration().versions().stream()
                .sorted().collect( Collectors.toList() );
        var benchmarkedProtocols = Arrays.stream( ProtocolVersion.values() ).filter( pv -> pv != LATEST )
                .map( ProtocolVersion::version ).collect( Collectors.toList() );
        assertEquals( productionProtocols, benchmarkedProtocols );
        assertEquals( LATEST.version(), productionProtocols.get( productionProtocols.size() - 1 ) );
    }

    @Test
    void storeCopyBare() throws Throwable
    {
        StoreCopyBare.DEBUG = true;
        var c = new StoreCopyBare();
        c.filesSize = "100kB";
        c.protocolVersion = LATEST;
        c.benchmarkSetup( null, null, null, null );
        c.copyStore();
        c.benchmarkTearDown();
    }

    @Test
    void storeCopyWithInfrastructure() throws Throwable
    {
        StoreCopyWithInfrastructure.DEBUG = true;
        var c = new StoreCopyWithInfrastructure()
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
        c.prefilledDatabaseSize = "100kB";
        c.protocolVersion = LATEST;
        c.pre();
        c.copyStore();
        c.post();
    }

    @Test
    void txPullBare() throws Throwable
    {
        TxPullBare.DEBUG = true;
        var c = new TxPullBare();
        c.txSize = "100kB";
        c.protocolVersion = LATEST;
        c.benchmarkSetup( null, null, null, null );
        c.pullTransactions();
        c.benchmarkTearDown();
    }

    @Test
    void txPullWithInfrastructure() throws Throwable
    {
        TxPullWithInfrastructure.DEBUG = true;
        var c = new TxPullWithInfrastructure()
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
        c.txSize = "100kB";
        c.protocolVersion = LATEST;
        c.pre();
        c.pullTransactions();
        c.post();
    }
}
