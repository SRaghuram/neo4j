/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.lock;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;

/**
 * Statement locks factory that will produce slave specific statement locks
 * that are aware how to grab shared locks for labels and relationship types
 * during slave commit
 */
public class SlaveStatementLocksFactory implements StatementLocksFactory
{
    private final StatementLocksFactory delegate;

    public SlaveStatementLocksFactory( StatementLocksFactory delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void initialize( Locks locks, Config config )
    {
        delegate.initialize( locks, config );
    }

    @Override
    public StatementLocks newInstance()
    {
        StatementLocks statementLocks = delegate.newInstance();
        return new SlaveStatementLocks( statementLocks );
    }
}
