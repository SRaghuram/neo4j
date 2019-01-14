/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster.modeswitch;

import java.util.function.Supplier;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.kernel.ha.DelegateInvocationHandler;
import org.neo4j.kernel.ha.SlaveRelationshipTypeCreator;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.kernel.impl.core.TokenCreator;

public class RelationshipTypeCreatorSwitcher extends AbstractComponentSwitcher<TokenCreator>
{
    private final DelegateInvocationHandler<Master> master;
    private final RequestContextFactory requestContextFactory;
    private final Supplier<Kernel> kernelSupplier;

    public RelationshipTypeCreatorSwitcher( DelegateInvocationHandler<TokenCreator> delegate,
            DelegateInvocationHandler<Master> master, RequestContextFactory requestContextFactory,
            Supplier<Kernel> kernelSupplier )
    {
        super( delegate );
        this.master = master;
        this.requestContextFactory = requestContextFactory;
        this.kernelSupplier = kernelSupplier;
    }

    @Override
    protected TokenCreator getMasterImpl()
    {
        return new DefaultRelationshipTypeCreator( kernelSupplier );
    }

    @Override
    protected TokenCreator getSlaveImpl()
    {
        return new SlaveRelationshipTypeCreator( master.cement(), requestContextFactory );
    }
}
