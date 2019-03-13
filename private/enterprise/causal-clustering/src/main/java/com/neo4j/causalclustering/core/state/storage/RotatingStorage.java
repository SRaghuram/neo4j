/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import org.neo4j.kernel.lifecycle.Lifecycle;

public interface RotatingStorage<T> extends StateStorage<T>, Lifecycle
{
    boolean exists();
}
