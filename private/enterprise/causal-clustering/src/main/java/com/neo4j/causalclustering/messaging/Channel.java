/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

import java.util.concurrent.Future;

public interface Channel
{
    void dispose();

    boolean isDisposed();

    boolean isOpen();

    Future<Void> write( Object msg );

    Future<Void> writeAndFlush( Object msg );
}
