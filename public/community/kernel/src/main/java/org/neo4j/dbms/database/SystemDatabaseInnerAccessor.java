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
package org.neo4j.dbms.database;

import java.util.Map;
import java.util.function.Supplier;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.GraphDatabaseQueryService;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.query.Neo4jTransactionalContextFactory;
import org.neo4j.kernel.impl.query.QueryExecutionKernelException;
import org.neo4j.kernel.impl.query.TransactionalContext;
import org.neo4j.kernel.impl.query.TransactionalContextFactory;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.virtual.MapValue;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;

public class SystemDatabaseInnerAccessor<DB extends DatabaseContext> implements Supplier<GraphDatabaseQueryService>
{
    private GraphDatabaseFacade systemDb;
    private SystemDatabaseInnerEngine engine;
    private TransactionalContextFactory contextFactory;

    public SystemDatabaseInnerAccessor( DatabaseManager<DB> databases, SystemDatabaseInnerEngine engine )
    {
        DatabaseId systemDatabaseId = new DatabaseId( SYSTEM_DATABASE_NAME );
        assert databases.getDatabaseContext( systemDatabaseId ).isPresent();
        this.systemDb = databases.getDatabaseContext( systemDatabaseId ).get().databaseFacade();
        this.engine = engine;
        ThreadToStatementContextBridge txBridge = this.systemDb.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
        this.contextFactory = Neo4jTransactionalContextFactory.create( systemDb, this, txBridge );
    }

    public InternalTransaction beginTx()
    {
        return systemDb.beginTransaction( KernelTransaction.Type.explicit, AUTH_DISABLED );
    }

    public Result execute( String query, Map<String,Object> params )
    {
        try
        {
            MapValue parameters = ValueUtils.asParameterMapValue( params );
            InternalTransaction transaction = systemDb.beginTransaction( KernelTransaction.Type.implicit, AUTH_DISABLED );
            TransactionalContext context = contextFactory.newContext( transaction, query, parameters );
            return this.engine.execute( query, parameters, context );
        }
        catch ( QueryExecutionKernelException e )
        {
            throw e.asUserException();
        }
    }

    @Override
    public GraphDatabaseQueryService get()
    {
        return systemDb.getDependencyResolver().resolveDependency( GraphDatabaseQueryService.class );
    }

    /**
     * The multidatabase system needs different access to the query execution engine for the system database when compared to normal databases.
     * User queries executed against the system database will only be understood if they are system administration commands. However, internally
     * these commands are converted into normal Cypher commands, which are also executed against the system database. To support this the
     * system database has two execution engines, the normal accessible from the outside only supports administration commands and no graph commands,
     * while the inner one which does understand graph commands is only available to key internal infrastructure like the security infrastructure.
     */
    public interface SystemDatabaseInnerEngine
    {
        Result execute( String query, MapValue parameters, TransactionalContext context ) throws QueryExecutionKernelException;
    }
}
