/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.pagecache;

import java.io.Closeable;
import java.io.IOException;

interface PageLoader extends Closeable
{
    void load( long pageId ) throws IOException;
}
