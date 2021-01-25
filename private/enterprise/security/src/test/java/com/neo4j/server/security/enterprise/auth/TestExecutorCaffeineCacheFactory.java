/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import org.neo4j.cypher.internal.cache.ExecutorBasedCaffeineCacheFactory;

public class TestExecutorCaffeineCacheFactory
{
    private static final ExecutorBasedCaffeineCacheFactory instance = new ExecutorBasedCaffeineCacheFactory( Runnable::run );

    private TestExecutorCaffeineCacheFactory()
    {
    }

    public static ExecutorBasedCaffeineCacheFactory getInstance()
    {
        return instance;
    }
}
