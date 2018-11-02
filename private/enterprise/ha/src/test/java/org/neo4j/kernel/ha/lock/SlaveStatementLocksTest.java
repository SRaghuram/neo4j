/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock;

import org.junit.Test;

import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.storageengine.api.lock.LockTracer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SlaveStatementLocksTest
{
    @Test
    public void acquireDeferredSharedLocksOnPrepareForCommit()
    {
        StatementLocks statementLocks = mock( StatementLocks.class );
        SlaveLocksClient slaveLocksClient = mock( SlaveLocksClient.class );
        when( statementLocks.optimistic() ).thenReturn( slaveLocksClient );

        SlaveStatementLocks slaveStatementLocks = new SlaveStatementLocks( statementLocks );
        slaveStatementLocks.prepareForCommit( LockTracer.NONE );

        verify( statementLocks ).prepareForCommit( LockTracer.NONE );
        verify( slaveLocksClient ).acquireDeferredSharedLocks( LockTracer.NONE );
    }
}
