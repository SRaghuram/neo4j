/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import java.io.File;
import java.util.Map;

import org.neo4j.io.pagecache.PageCache;

public interface TemporaryDatabaseFactory
{
    TemporaryDatabase startTemporaryDatabase( PageCache pageCache, File rootDirectory, Map<String,String> params );
}
