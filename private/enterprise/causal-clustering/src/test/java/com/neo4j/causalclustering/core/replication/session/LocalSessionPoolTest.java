/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication.session;

import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

class LocalSessionPoolTest
{
    private MemberId memberId = IdFactory.randomMemberId();
    private GlobalSession globalSession = new GlobalSession( UUID.randomUUID(), memberId );

    @Test
    void poolGivesBackSameSessionAfterRelease()
    {
        LocalSessionPool sessionPool = new LocalSessionPool( globalSession );

        OperationContext contextA = sessionPool.acquireSession();
        sessionPool.releaseSession( contextA );

        OperationContext contextB = sessionPool.acquireSession();
        sessionPool.releaseSession( contextB );

        Assertions.assertEquals( contextA.localSession(), contextB.localSession() );
    }

    @Test
    void sessionAcquirementIncreasesOperationId()
    {
        LocalSessionPool sessionPool = new LocalSessionPool( globalSession );
        OperationContext context;

        context = sessionPool.acquireSession();
        LocalOperationId operationA = context.localOperationId();
        sessionPool.releaseSession( context );

        context = sessionPool.acquireSession();
        LocalOperationId operationB = context.localOperationId();
        sessionPool.releaseSession( context );

        Assertions.assertEquals( operationB.sequenceNumber(), operationA.sequenceNumber() + 1 );
    }

    @Test
    void poolHasIndependentSessions()
    {
        LocalSessionPool sessionPool = new LocalSessionPool( globalSession );

        OperationContext contextA = sessionPool.acquireSession();
        OperationContext contextB = sessionPool.acquireSession();

        Assertions.assertNotEquals( contextA.localSession(), contextB.localSession() );
    }
}
