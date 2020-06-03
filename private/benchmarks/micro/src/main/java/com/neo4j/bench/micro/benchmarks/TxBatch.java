/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class TxBatch
{
    private final GraphDatabaseService db;
    private final int txBatchSize;
    private int txSize;
    private Transaction tx;

    public TxBatch( GraphDatabaseService db, int txBatchSize )
    {
        this.db = db;
        this.txBatchSize = txBatchSize;
        this.txSize = 0;
    }

    public Transaction transaction()
    {
        return tx;
    }

    public boolean advance()
    {
        if ( null == tx )
        {
            tx = beginTransaction();
            return true;
        }
        if ( ++txSize == txBatchSize )
        {
            tx = commitAndNew( tx );
            txSize = 0;
            return true;
        }
        return false;
    }

    protected Transaction beginTransaction()
    {
        return db.beginTx();
    }

    public void close()
    {
        commit( tx );
    }

    private Transaction commitAndNew( Transaction oldTx )
    {
        commit( oldTx );
        return beginTransaction();
    }

    private void commit( Transaction oldTx )
    {
        try
        {
            oldTx.commit();
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
            throw new RuntimeException( "Unable to commit transaction", e );
        }
        finally
        {
            try
            {
                oldTx.close();
            }
            catch ( Throwable e )
            {
                e.printStackTrace();
                throw new RuntimeException( "Unable to close transaction", e );
            }
        }
    }
}
