/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import org.junit.jupiter.api.Test;

class RaftTest
{
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
        c.protocolVersion = ProtocolVersion.LATEST;
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
        c.protocolVersion = ProtocolVersion.LATEST;
        c.txSize = "100kb";
        c.pre();
        c.serializeContent();
        c.post();
    }
}
