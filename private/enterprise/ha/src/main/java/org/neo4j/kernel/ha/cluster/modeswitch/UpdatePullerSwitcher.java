/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.MasterUpdatePuller;
import org.neo4j.kernel.ha.PullerFactory;
import org.neo4j.kernel.ha.SlaveUpdatePuller;
import org.neo4j.kernel.ha.UpdatePuller;

/**
 * UpdatePullerSwitcher will provide different implementations of {@link UpdatePuller}
 * depending on node mode (slave or master).
 *
 * @see UpdatePuller
 * @see SlaveUpdatePuller
 */
public class UpdatePullerSwitcher extends AbstractComponentSwitcher<UpdatePuller>
{
    private final PullerFactory pullerFactory;

    public UpdatePullerSwitcher( DelegateInvocationHandler<UpdatePuller> delegate, PullerFactory pullerFactory )
    {
        super( delegate );
        this.pullerFactory = pullerFactory;
    }

    @Override
    protected UpdatePuller getMasterImpl()
    {
        return MasterUpdatePuller.INSTANCE;
    }

    @Override
    protected UpdatePuller getSlaveImpl()
    {
        return pullerFactory.createSlaveUpdatePuller();
    }

    @Override
    protected void shutdownOldDelegate( UpdatePuller updatePuller )
    {
        if ( updatePuller != null )
        {
            updatePuller.stop();
        }
    }

    @Override
    protected void startNewDelegate( UpdatePuller updatePuller )
    {
        if ( updatePuller != null )
        {
            updatePuller.start();
        }
    }

}
