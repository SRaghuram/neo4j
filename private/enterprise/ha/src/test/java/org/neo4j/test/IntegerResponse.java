/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.test;

import org.neo4j.com.Response;
import org.neo4j.storageengine.api.StoreId;

public class IntegerResponse extends Response<Integer>
{
    public IntegerResponse( Integer response )
    {
        super( response, StoreId.DEFAULT, () ->
        {
        } );
    }

    @Override
    public void accept( Handler handler )
    {
    }

    @Override
    public boolean hasTransactionsToBeApplied()
    {
        return false;
    }
}
