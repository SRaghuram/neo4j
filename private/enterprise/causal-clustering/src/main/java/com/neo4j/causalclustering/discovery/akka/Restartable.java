/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import java.util.concurrent.Future;

public interface Restartable
{
    /**
     * Schedule restart to be performed asynchronously
     *
     * @return A future that completes when the restart operation is finished
     */
    Future<?> scheduleRestart();
}
