/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.ldbc;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;

import java.io.Closeable;

public interface Neo4jDbCommands extends Closeable
{
    void init() throws DbException;

    DbConnectionState getConnectionState() throws DbException;

    void registerHandlersWithDb( Db db ) throws DbException;
}
