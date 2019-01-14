/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock;

import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.com.RequestContextFactory;
import org.neo4j.kernel.ha.com.master.Master;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.logging.LogProvider;

public class SlaveLockManager implements Locks
{
    private final RequestContextFactory requestContextFactory;
    private final Locks local;
    private final Master master;
    private final AvailabilityGuard availabilityGuard;
    private final LogProvider logProvider;

    public SlaveLockManager( Locks localLocks, RequestContextFactory requestContextFactory, Master master,
            AvailabilityGuard availabilityGuard, LogProvider logProvider, Config config )
    {
        this.requestContextFactory = requestContextFactory;
        this.availabilityGuard = availabilityGuard;
        this.local = localLocks;
        this.master = master;
        this.logProvider = logProvider;
    }

    @Override
    public Client newClient()
    {
        Client client = local.newClient();
        return new SlaveLocksClient( master, client, local, requestContextFactory, availabilityGuard, logProvider );
    }

    @Override
    public void accept( Visitor visitor )
    {
        local.accept( visitor );
    }

    @Override
    public void close()
    {
        local.close();
    }
}
