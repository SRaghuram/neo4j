/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise.lock.forseti;

import java.time.Clock;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.LocksFactory;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;

@ServiceProvider
public class ForsetiLocksFactory implements LocksFactory
{
    public static final String KEY = "forseti";

    @Override
    public String getName()
    {
        return KEY;
    }

    @Override
    public int getPriority()
    {
        return 10;
    }

    @Override
    public Locks newInstance( Config config, Clock clock, ResourceType[] resourceTypes )
    {
        return new ForsetiLockManager( config, clock, ResourceTypes.values() );
    }
}
