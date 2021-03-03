/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.execution.database;

import com.neo4j.bench.common.process.Pid;
import com.neo4j.bench.model.model.PlanOperator;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;

import org.neo4j.driver.Session;

public class DelegatingServerDatabase implements ServerDatabase
{
    private final ServerDatabase delegate;
    private final Closeable closeable;

    public DelegatingServerDatabase( ServerDatabase delegate, Closeable closeable )
    {
        this.delegate = delegate;
        this.closeable = closeable;
    }

    @Override
    public URI boltUri()
    {
        return delegate.boltUri();
    }

    @Override
    public Session session()
    {
        return delegate.session();
    }

    @Override
    public Pid pid()
    {
        return delegate.pid();
    }

    @Override
    public PlanOperator executeAndGetPlan( String query, Map<String,Object> parameters, boolean executeInTx, boolean shouldRollback )
    {
        return delegate.executeAndGetPlan( query, parameters, executeInTx, shouldRollback );
    }

    @Override
    public int executeAndGetRows( String query, Map<String,Object> parameters, boolean executeInTx, boolean shouldRollback )
    {
        return delegate.executeAndGetRows( query, parameters, executeInTx, shouldRollback );
    }

    @Override
    public void close()
    {
        delegate.close();
        try
        {
            closeable.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }
}
