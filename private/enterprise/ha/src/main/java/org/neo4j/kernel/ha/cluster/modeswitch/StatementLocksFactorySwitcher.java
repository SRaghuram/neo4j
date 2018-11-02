/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster.modeswitch;

import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.lock.SlaveStatementLocksFactory;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

/**
 * Statement locks factory switcher that will use original configured locks factory in case if
 * instance is master, other wise slave specific locks factory will be provided that have additional
 * capabilities of acquiring some shared locks on master during commit
 */
public class StatementLocksFactorySwitcher extends AbstractComponentSwitcher<StatementLocksFactory>
{
    private final StatementLocksFactory configuredStatementLocksFactory;

    public StatementLocksFactorySwitcher( DelegateInvocationHandler<StatementLocksFactory> delegate,
            StatementLocksFactory configuredStatementLocksFactory )
    {
        super( delegate );
        this.configuredStatementLocksFactory = configuredStatementLocksFactory;
    }

    @Override
    protected StatementLocksFactory getMasterImpl()
    {
        return configuredStatementLocksFactory;
    }

    @Override
    protected StatementLocksFactory getSlaveImpl()
    {
        return new SlaveStatementLocksFactory( configuredStatementLocksFactory );
    }
}
