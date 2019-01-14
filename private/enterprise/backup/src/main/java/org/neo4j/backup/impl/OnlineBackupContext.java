/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.impl;

import java.nio.file.Path;

import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.kernel.configuration.Config;

class OnlineBackupContext
{
    private final OnlineBackupRequiredArguments requiredArguments;
    private final Config config;
    private final ConsistencyFlags consistencyFlags;

    OnlineBackupContext( OnlineBackupRequiredArguments requiredArguments, Config config, ConsistencyFlags consistencyFlags )
    {
        this.requiredArguments = requiredArguments;
        this.config = config;
        this.consistencyFlags = consistencyFlags;
    }

    public OnlineBackupRequiredArguments getRequiredArguments()
    {
        return requiredArguments;
    }

    public Config getConfig()
    {
        return config;
    }

    public ConsistencyFlags getConsistencyFlags()
    {
        return consistencyFlags;
    }

    public Path getResolvedLocationFromName()
    {
        return requiredArguments.getResolvedLocationFromName();
    }
}
