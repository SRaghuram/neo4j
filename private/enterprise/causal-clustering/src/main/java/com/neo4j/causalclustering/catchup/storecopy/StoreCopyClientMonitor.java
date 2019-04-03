/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

public interface StoreCopyClientMonitor
{
    void startReceivingStoreFiles();

    void finishReceivingStoreFiles();

    void startReceivingStoreFile( String file );

    void finishReceivingStoreFile( String file );

    void startReceivingTransactions( long startTxId );

    void finishReceivingTransactions( long endTxId );

    class Adapter implements StoreCopyClientMonitor
    {
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

    }
}
