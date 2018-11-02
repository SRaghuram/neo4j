/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.MasterUpdatePuller;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class UpdatePullerSwitcherTest
{
    private UpdatePullerSwitcher modeSwitcher;
    private SlaveUpdatePuller slaveUpdatePuller;

    @Before
    public void setUp()
    {
        @SuppressWarnings( "unchecked" )
        DelegateInvocationHandler<UpdatePuller> invocationHandler = mock( DelegateInvocationHandler.class );
        PullerFactory pullerFactory = mock( PullerFactory.class );
        slaveUpdatePuller = mock( SlaveUpdatePuller.class );
        when( pullerFactory.createSlaveUpdatePuller() ).thenReturn( slaveUpdatePuller );
        when( invocationHandler.setDelegate( slaveUpdatePuller ) ).thenReturn( slaveUpdatePuller );

        modeSwitcher = new UpdatePullerSwitcher( invocationHandler, pullerFactory );
    }

    @Test
    public void masterUpdatePuller()
    {
        UpdatePuller masterPuller = modeSwitcher.getMasterImpl();
        assertSame( MasterUpdatePuller.INSTANCE, masterPuller );
    }

    @Test
    public void slaveUpdatePuller()
    {
        UpdatePuller updatePuller = modeSwitcher.getSlaveImpl();
        assertSame( slaveUpdatePuller, updatePuller );
        verifyZeroInteractions( slaveUpdatePuller );
    }

    @Test
    public void switchToPendingTest()
    {
        modeSwitcher.switchToSlave();
        verify( slaveUpdatePuller ).start();

        modeSwitcher.switchToSlave();
        InOrder inOrder = inOrder( slaveUpdatePuller );
        inOrder.verify( slaveUpdatePuller ).stop();
        inOrder.verify( slaveUpdatePuller ).start();
    }
}
