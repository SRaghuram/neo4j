/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SlaveStatementLocksFactoryTest
{

    @Test
    public void createSlaveStatementLocks()
    {
        StatementLocksFactory delegate = mock( StatementLocksFactory.class );
        Locks locks = mock( Locks.class );
        Config config = Config.defaults();

        SlaveStatementLocksFactory slaveStatementLocksFactory = new SlaveStatementLocksFactory( delegate );
        slaveStatementLocksFactory.initialize( locks, config );
        StatementLocks statementLocks = slaveStatementLocksFactory.newInstance();

        assertThat( statementLocks, instanceOf( SlaveStatementLocks.class ) );
        verify( delegate ).initialize( locks, config );
        verify( delegate ).newInstance();
    }
}
