/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.snapshot;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Handle snapshot data for cluster state.
 */
public interface SnapshotProvider
{
    void getState( ObjectOutputStream output ) throws IOException;

    void setState( ObjectInputStream input ) throws IOException, ClassNotFoundException;
}
