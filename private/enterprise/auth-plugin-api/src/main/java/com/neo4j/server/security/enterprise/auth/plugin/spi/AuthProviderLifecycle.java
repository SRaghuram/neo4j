/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.spi;

import com.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;

public interface AuthProviderLifecycle
{
    void initialize( AuthProviderOperations authProviderOperations ) throws Exception;
    void start();
    void stop();
    void shutdown();

    class Adapter implements AuthProviderLifecycle
    {
        @Override
        public void initialize( AuthProviderOperations authProviderOperations )
        {
        }

        @Override
        public void start()
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void shutdown()
        {
        }
    }
}
