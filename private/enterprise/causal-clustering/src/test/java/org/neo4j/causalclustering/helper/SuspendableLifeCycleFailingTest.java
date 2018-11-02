/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.function.ThrowingConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SuspendableLifeCycleFailingTest
{

    private CountingThrowingSuspendableLifeCycle lifeCycle;

    @Before
    public void setup() throws Throwable
    {
        lifeCycle = new CountingThrowingSuspendableLifeCycle();
        lifeCycle.init();
    }

    @Test
    public void canEnableIfStopFailed() throws Throwable
    {
        lifeCycle.start();
        lifeCycle.setFailMode();

        runFailing( SuspendableLifeCycle::stop );

        lifeCycle.setSuccessMode();

        lifeCycle.enable();

        assertEquals( 2, lifeCycle.starts );
    }

    @Test
    public void canEnableIfShutdownFailed() throws Throwable
    {
        lifeCycle.start();
        lifeCycle.setFailMode();

        runFailing( SuspendableLifeCycle::shutdown );

        lifeCycle.setSuccessMode();

        lifeCycle.enable();

        assertEquals( 2, lifeCycle.starts );
    }

    @Test
    public void canStartifDisableFailed() throws Throwable
    {
        lifeCycle.setFailMode();
        runFailing( SuspendableLifeCycle::disable );

        lifeCycle.setSuccessMode();
        lifeCycle.start();

        assertEquals( 1, lifeCycle.starts );
    }

    private void runFailing( ThrowingConsumer<SuspendableLifeCycle,Throwable> consumer ) throws Throwable
    {
        try
        {
            consumer.accept( lifeCycle );
            fail();
        }
        catch ( IllegalStateException ignore )
        {
        }
    }
}
