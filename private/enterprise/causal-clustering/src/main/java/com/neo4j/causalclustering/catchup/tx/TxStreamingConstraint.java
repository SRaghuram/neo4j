/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.tx;

public interface TxStreamingConstraint
{
    boolean shouldContinue( long lastSentTxId );

    class Limited implements TxStreamingConstraint
    {
        private final long lastTxIdToSend;

        public Limited( long lastTxIdToSend )
        {
            this.lastTxIdToSend = lastTxIdToSend;
        }

        @Override
        public boolean shouldContinue( long lastSentTxId )
        {
            return lastSentTxId < lastTxIdToSend;
        }
    }

    class Unbounded implements TxStreamingConstraint
    {
        @Override
        public boolean shouldContinue( long lastSentTxId )
        {
            return true;
        }
    }
}
