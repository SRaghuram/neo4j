/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.proc;

import java.time.Clock;

import org.neo4j.common.DependencyResolver;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.ValueMapper;

/**
 * The context in which a procedure is invoked. This is a read-only map-like structure.
 * For instance, a read-only transactional procedure might have access to the current statement it is being invoked
 * in through this.
 *
 * The context is entirely defined by the caller of the procedure,
 * so what is available in the context depends on the context of the call.
 */
public interface Context
{
    String DEPENDENCY_RESOLVER_NAME = "DependencyResolver";
    String DATABASE_API_NAME = "DatabaseAPI";
    String KERNEL_TRANSACTION_NAME = "KernelTransaction";
    String VALUE_MAPPER_NAME = "ValueMapper";
    String SECURITY_CONTEXT_NAME = "SecurityContext";
    String THREAD_NAME = "Thread";
    String SYSTEM_CLOCK_NAME = "SystemClock";
    String STATEMENT_CLOCK_NAME = "StatementClock";
    String TRANSACTION_CLOCK_NAME = "TransactionClock";

    Key<DependencyResolver> DEPENDENCY_RESOLVER = Key.key( DEPENDENCY_RESOLVER_NAME, DependencyResolver.class );
    Key<GraphDatabaseAPI> DATABASE_API = Key.key( DATABASE_API_NAME, GraphDatabaseAPI.class );
    Key<KernelTransaction> KERNEL_TRANSACTION = Key.key( KERNEL_TRANSACTION_NAME, KernelTransaction.class );
    Key<ValueMapper> VALUE_MAPPER = Key.key( VALUE_MAPPER_NAME, ValueMapper.class );
    Key<SecurityContext> SECURITY_CONTEXT = Key.key( SECURITY_CONTEXT_NAME, SecurityContext.class );
    Key<Thread> THREAD = Key.key( THREAD_NAME, Thread.class );
    Key<Clock> SYSTEM_CLOCK = Key.key( SYSTEM_CLOCK_NAME, Clock.class );
    Key<Clock> STATEMENT_CLOCK = Key.key( STATEMENT_CLOCK_NAME, Clock.class );
    Key<Clock> TRANSACTION_CLOCK = Key.key( TRANSACTION_CLOCK_NAME, Clock.class );

    <T> T get( Key<T> key ) throws ProcedureException;
    <T> T getOrElse( Key<T> key, T orElse );

    //I don't see much of a point of calling via a generic get method
    //so I think we should move towards real getters.

    ValueMapper<Object> valueMapper();
}
