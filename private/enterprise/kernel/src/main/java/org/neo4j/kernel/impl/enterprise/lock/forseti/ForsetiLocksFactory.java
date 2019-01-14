/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import java.time.Clock;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.DynamicLocksFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.ResourceType;

@Service.Implementation( DynamicLocksFactory.class )
public class ForsetiLocksFactory extends DynamicLocksFactory
{
    public static final String KEY = "forseti";

    public ForsetiLocksFactory()
    {
        super( KEY );
    }

    @Override
    public Locks newInstance( Config config, Clock clock, ResourceType[] resourceTypes )
    {
        return new ForsetiLockManager( config, clock, ResourceTypes.values() );
    }
}
