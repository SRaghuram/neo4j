/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.Token;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;

import static org.neo4j.internal.kernel.api.Transaction.Type.implicit;

public class KernelTxBatch
{
    private final int txBatchSize;
    private final Kernel kernel;
    private int txSize;
    private Transaction tx;
    public CursorFactory cursors;
    public Read read;
    public Write write;
    public SchemaRead schemaRead;
    public Token token;

    public KernelTxBatch( Kernel kernel, int txBatchSize ) throws InvalidTransactionTypeKernelException, TransactionFailureException
    {
        this.txBatchSize = txBatchSize;
        this.txSize = 0;
        this.kernel = kernel;
        newTx();
    }

    public void advance() throws TransactionFailureException, InvalidTransactionTypeKernelException
    {
        if ( ++txSize == txBatchSize )
        {
            commitAndNew( tx );
            txSize = 0;
        }
    }

    public void closeTx() throws Exception
    {
        tx.close();
        cursors = null;
        token = null;
    }

    private void commitAndNew( Transaction oldTx )
            throws TransactionFailureException, InvalidTransactionTypeKernelException
    {
        commit( oldTx );
        newTx();
    }

    private void newTx() throws TransactionFailureException, InvalidTransactionTypeKernelException
    {
        this.tx = kernel.beginTransaction( implicit, SecurityContext.AUTH_DISABLED );
        cursors = tx.cursors();
        token = tx.token();
        read = tx.dataRead();
        write = tx.dataWrite();
        schemaRead = tx.schemaRead();
    }

    private void commit( Transaction oldTx ) throws TransactionFailureException
    {
        try
        {
            oldTx.success();
        }
        finally
        {
            oldTx.close();
        }
    }
}
