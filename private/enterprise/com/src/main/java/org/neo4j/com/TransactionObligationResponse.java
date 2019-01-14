/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.io.IOException;

import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.storageengine.api.StoreId;

/**
 * {@link Response} that carries transaction obligation as a side-effect.
 *
 * @see TransactionObligationFulfiller
 */
public class TransactionObligationResponse<T> extends Response<T>
{
    static final byte RESPONSE_TYPE = -1;

    static final Response<Void> EMPTY_RESPONSE = new TransactionObligationResponse<>( null, StoreId.DEFAULT,
            -1, ResourceReleaser.NO_OP );

    private final long obligationTxId;

    public TransactionObligationResponse( T response, StoreId storeId, long obligationTxId, ResourceReleaser releaser )
    {
        super( response, storeId, releaser );
        this.obligationTxId = obligationTxId;
    }

    @Override
    public void accept( Response.Handler handler ) throws IOException
    {
        handler.obligation( obligationTxId );
    }

    @Override
    public boolean hasTransactionsToBeApplied()
    {
        return false;
    }
}
