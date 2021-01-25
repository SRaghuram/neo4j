/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

/**
 * Represents the action to take upon the expiry of a timer.
 */
@FunctionalInterface
public interface TimeoutHandler
{
    void onTimeout( Timer timer ) throws Exception;
}
