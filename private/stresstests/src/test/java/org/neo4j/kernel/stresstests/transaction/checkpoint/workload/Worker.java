/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.workload;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.stresstests.transaction.checkpoint.mutation.RandomMutation;

class Worker implements Runnable
{
    interface Monitor
    {
        void transactionCompleted();
        boolean stop();
        void done();
    }

    private final GraphDatabaseService db;
    private final RandomMutation randomMutation;
    private final Monitor monitor;
    private final int numOpsPerTx;

    Worker( GraphDatabaseService db, RandomMutation randomMutation, Monitor monitor, int numOpsPerTx )
    {
        this.db = db;
        this.randomMutation = randomMutation;
        this.monitor = monitor;
        this.numOpsPerTx = numOpsPerTx;
    }

    @Override
    public void run()
    {
        do
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int i = 0; i < numOpsPerTx; i++ )
                {
                    randomMutation.perform();
                }
                tx.success();
            }
            catch ( DeadlockDetectedException ignore )
            {
                // simply give up
            }
            catch ( Exception e )
            {
                // ignore and go on
                e.printStackTrace();
            }

            monitor.transactionCompleted();
        }
        while ( !monitor.stop() );

        monitor.done();
    }
}
