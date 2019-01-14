/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.slave;

import org.neo4j.com.Response;
import org.neo4j.com.storecopy.TransactionObligationFulfiller;
import org.neo4j.kernel.ha.com.master.Slave;

public class SlaveImpl implements Slave
{
    private final TransactionObligationFulfiller fulfiller;

    public SlaveImpl( TransactionObligationFulfiller fulfiller )
    {
        this.fulfiller = fulfiller;
    }

    @Override
    public Response<Void> pullUpdates( long upToAndIncludingTxId )
    {
        try
        {
            fulfiller.fulfill( upToAndIncludingTxId );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        return Response.empty();
    }

    @Override
    public int getServerId()
    {
        throw new UnsupportedOperationException( "This should not be called. Knowing the server id is only needed " +
                "on the client side, we're now on the server side." );
    }
}
