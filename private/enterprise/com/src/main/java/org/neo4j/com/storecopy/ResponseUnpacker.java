/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import org.neo4j.com.Response;

public interface ResponseUnpacker
{
    /**
     * @param txHandler for getting an insight into which transactions gets applied.
     */
    void unpackResponse( Response<?> response, TxHandler txHandler ) throws Exception;

    ResponseUnpacker NO_OP_RESPONSE_UNPACKER = ( response, txHandler ) ->
    {
        /* Do nothing */
    };

    interface TxHandler
    {
        TxHandler NO_OP_TX_HANDLER = transactionId ->
        {
            /* Do nothing */
        };

        void accept( long transactionId );
    }
}
