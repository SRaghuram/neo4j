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

    public boolean advance()
    {
        if ( null == tx )
        {
            tx = db.beginTx();
            return true;
        }
        if ( ++txSize == txBatchSize )
        {
            tx = commitAndNew( db, tx );
            txSize = 0;
            return true;
        }
        return false;
    }

    public void close()
    {
        commit( tx );
    }

    private Transaction commitAndNew( GraphDatabaseService db, Transaction oldTx )
    {
        commit( oldTx );
        return db.beginTx();
    }

    private void commit( Transaction oldTx )
    {
        try
        {
            oldTx.success();
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
