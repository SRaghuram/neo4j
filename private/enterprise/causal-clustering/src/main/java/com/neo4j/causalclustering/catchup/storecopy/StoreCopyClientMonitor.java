/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

public interface StoreCopyClientMonitor
{
    void start();

    void startReceivingStoreFiles();

    void finishReceivingStoreFiles();

    void startReceivingStoreFile( String file );

    void finishReceivingStoreFile( String file );

    void startReceivingTransactions( long startTxId );

    void finishReceivingTransactions( long endTxId );

    void startRecoveringStore();

    void finishRecoveringStore();

    void startReceivingIndexSnapshots();

    void startReceivingIndexSnapshot( long indexId );

    void finishReceivingIndexSnapshot( long indexId );

    void finishReceivingIndexSnapshots();

    void finish();

    class Adapter implements StoreCopyClientMonitor
    {
        @Override
        public void start()
        {   // empty
        }

        @Override
        public void startReceivingStoreFiles()
        {   // empty
        }

        @Override
        public void finishReceivingStoreFiles()
        {   // empty
        }

        @Override
        public void startReceivingStoreFile( String file )
        {   // empty
        }

        @Override
        public void finishReceivingStoreFile( String file )
        {   // empty
        }

        @Override
        public void startReceivingTransactions( long startTxId )
        {   // empty
        }

        @Override
        public void finishReceivingTransactions( long endTxId )
        {   // empty
        }

        @Override
        public void startRecoveringStore()
        {   // empty
        }

        @Override
        public void finishRecoveringStore()
        {   // empty
        }

        @Override
        public void startReceivingIndexSnapshots()
        {   // empty
        }

        @Override
        public void startReceivingIndexSnapshot( long indexId )
        {   // empty
        }

        @Override
        public void finishReceivingIndexSnapshot( long indexId )
        {   // empty
        }

        @Override
        public void finishReceivingIndexSnapshots()
        {   // empty
        }

        @Override
        public void finish()
        {   // empty
        }
    }
}
