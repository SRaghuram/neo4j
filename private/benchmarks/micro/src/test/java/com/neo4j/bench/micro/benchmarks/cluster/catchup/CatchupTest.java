/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import org.junit.jupiter.api.Test;

class CatchupTest
{
    @Test
    void storeCopyBare() throws Throwable
    {
        StoreCopyBare.DEBUG = true;
        var c = new StoreCopyBare();
        c.filesSize = "1MB";
        c.protocolVersion = ProtocolVersion.LATEST;
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
        c.protocolVersion = ProtocolVersion.LATEST;
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
        c.protocolVersion = ProtocolVersion.LATEST;
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
        c.protocolVersion = ProtocolVersion.LATEST;
        c.pre();
        c.pullTransactions();
        c.post();
    }
}
