/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache.iocontroller;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.kernel.impl.pagecache.IOControllerService;
import org.neo4j.time.SystemNanoClock;

@ServiceProvider
public class ConfigurableIOControllerService implements IOControllerService
{
    @Override
    public IOController createIOController( Config config, SystemNanoClock clock )
    {
        return new ConfigurableIOController( config, clock );
    }

    @Override
    public int getPriority()
    {
        return 2;
    }
}
