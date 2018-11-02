/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com;

import java.util.function.Supplier;

import org.neo4j.com.RequestContext;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class RequestContextFactory extends LifecycleAdapter
{
    private long epoch;
    private final int serverId;
    private final Supplier<TransactionIdStore> txIdStoreSupplier;
    private TransactionIdStore txIdStore;

    public static final int DEFAULT_EVENT_IDENTIFIER = -1;

    public RequestContextFactory( int serverId, Supplier<TransactionIdStore> txIdStoreSupplier )
    {
        this.txIdStoreSupplier = txIdStoreSupplier;
        this.epoch = -1;
        this.serverId = serverId;
    }

    @Override
    public void start()
    {
        this.txIdStore = txIdStoreSupplier.get();
    }

    @Override
    public void stop()
    {
        this.txIdStore = null;
    }

    public void setEpoch( long epoch )
    {
        this.epoch = epoch;
    }

    public RequestContext newRequestContext( long epoch, int machineId, int eventIdentifier )
    {
        TransactionId lastTx = txIdStore.getLastCommittedTransaction();
        // TODO beware, there's a race between getting tx id and checksum, and changes to last tx
        // it must be fixed
        return new RequestContext( epoch, machineId, eventIdentifier, lastTx.transactionId(), lastTx.checksum() );
    }

    public RequestContext newRequestContext( int eventIdentifier )
    {
        return newRequestContext( epoch, serverId, eventIdentifier );
    }

    public RequestContext newRequestContext()
    {
        return newRequestContext( DEFAULT_EVENT_IDENTIFIER );
    }
}
