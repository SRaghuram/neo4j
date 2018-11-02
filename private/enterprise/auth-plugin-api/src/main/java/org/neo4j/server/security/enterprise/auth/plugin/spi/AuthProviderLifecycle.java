/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.plugin.spi;

import org.neo4j.server.security.enterprise.auth.plugin.api.AuthProviderOperations;

public interface AuthProviderLifecycle
{
    void initialize( AuthProviderOperations authProviderOperations ) throws Throwable;
    void start();
    void stop();
    void shutdown();

    class Adapter implements AuthProviderLifecycle
    {
        @Override
        public void initialize( AuthProviderOperations authProviderOperations ) throws Throwable
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
