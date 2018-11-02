/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

public interface RealmLifecycle
{
    void initialize() throws Throwable;
    void start() throws Throwable;
    void stop() throws Throwable;
    void shutdown() throws Throwable;

    class Adapter implements RealmLifecycle
    {
        @Override
        public void initialize()
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
