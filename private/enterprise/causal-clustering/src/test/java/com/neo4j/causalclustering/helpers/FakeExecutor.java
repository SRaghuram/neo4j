/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helpers;

import java.util.concurrent.Executor;
import javax.annotation.Nonnull;

public class FakeExecutor implements Executor
{
    @Override
    public void execute( @Nonnull Runnable command )
    {
        command.run();
    }
}
