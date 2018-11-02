/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.com.slave;

import org.neo4j.com.ComExceptionHandler;
import org.neo4j.com.Deserializer;
import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.ProtocolVersion;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.storecopy.ResponseUnpacker.TxHandler;
import org.neo4j.com.storecopy.StoreWriter;
import org.neo4j.kernel.ha.MasterClient320;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.ha.lock.LockResult;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

public interface MasterClient extends Master
{
    ProtocolVersion CURRENT = MasterClient320.PROTOCOL_VERSION;

    @Override
    Response<Integer> createRelationshipType( RequestContext context, String name );

    @Override
    Response<Void> newLockSession( RequestContext context );

    @Override
    Response<Long> commit( RequestContext context, TransactionRepresentation channel );

    @Override
    Response<Void> pullUpdates( RequestContext context );

    Response<Void> pullUpdates( RequestContext context, TxHandler txHandler );

    @Override
    Response<Void> copyStore( RequestContext context, StoreWriter writer );

    void setComExceptionHandler( ComExceptionHandler handler );

    ProtocolVersion getProtocolVersion();

    ObjectSerializer<LockResult> createLockResultSerializer();

    Deserializer<LockResult> createLockResultDeserializer();
}
