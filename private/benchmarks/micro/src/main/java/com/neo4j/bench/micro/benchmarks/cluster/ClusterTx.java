/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster;

import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

public class ClusterTx
{
    private final TransactionRepresentation transactionRepresentation;
    private final int size;

    ClusterTx( long txId, TransactionRepresentation transactionRepresentation, int size )
    {
        this.transactionRepresentation = transactionRepresentation;
        this.size = size;
    }

    public int size()
    {
        return size;
    }

    public TransactionRepresentation txRepresentation()
    {
        return transactionRepresentation;
    }
}
