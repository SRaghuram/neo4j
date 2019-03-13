/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

public interface RealmLifecycle
{
    void initialize() throws Exception;
    void start() throws Exception;
    void stop() throws Exception;
    void shutdown() throws Exception;

    class Adapter implements RealmLifecycle
    {
        @Override
        public void initialize() throws Exception
        {
        }

        @Override
        public void start() throws Exception
        {
        }

        @Override
        public void stop() throws Exception
        {
        }

        @Override
        public void shutdown()
        {
        }
    }
}
