/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.neo4j.com.storecopy.ResponseUnpacker;
import org.neo4j.storageengine.api.StoreId;

/**
 * {@link Response} that carries {@link TransactionStream transaction data} as a side-effect, to be applied
 * before accessing the response value.
 *
 * @see ResponseUnpacker
 */
public class TransactionStreamResponse<T> extends Response<T>
{
    private final TransactionStream transactions;

    public TransactionStreamResponse( T response, StoreId storeId, TransactionStream transactions,
            ResourceReleaser releaser )
    {
        super( response, storeId, releaser );
        this.transactions = transactions;
    }

    @Override
    public void accept( Response.Handler handler ) throws Exception
    {
        transactions.accept( handler.transactions() );
    }

    @Override
    public boolean hasTransactionsToBeApplied()
    {
        return true;
    }
}
