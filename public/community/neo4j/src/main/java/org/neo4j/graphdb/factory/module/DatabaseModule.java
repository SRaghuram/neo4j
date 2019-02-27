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
package org.neo4j.graphdb.factory.module;

import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseContext;
import org.neo4j.kernel.api.InwardKernel;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.coreapi.CoreAPIAvailabilityGuard;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.storageengine.api.StoreId;

public class DatabaseModule
{
    public final Database database;

    public final Supplier<InwardKernel> kernelAPI;

    public final Supplier<StoreId> storeId;

    public final CoreAPIAvailabilityGuard coreAPIAvailabilityGuard;

    public DatabaseModule( String databaseName, GlobalModule globalModule, AbstractEditionModule editionModule, GlobalProcedures globalProcedures,
            GraphDatabaseFacade graphDatabaseFacade )
    {
        EditionDatabaseContext editionContext = editionModule.createDatabaseContext( databaseName );
        ModularDatabaseCreationContext context =
                new ModularDatabaseCreationContext( databaseName, globalModule, editionContext, globalProcedures, graphDatabaseFacade );
        database = new Database( context );

        this.coreAPIAvailabilityGuard = context.getCoreAPIAvailabilityGuard();
        this.storeId = database::getStoreId;
        this.kernelAPI = database::getKernel;

        // TODO: this is incorrect, and we should remove procedure specific service and factory
        //  as soon as we will split database and dbms operations into separate services
        ProcedureGDSFactory gdsFactory =
                new ProcedureGDSFactory( globalModule, this, coreAPIAvailabilityGuard, context.getTokenHolders(),
                        editionModule.getThreadToTransactionBridge() );
        globalProcedures.registerComponent( GraphDatabaseService.class, gdsFactory::apply, true );
    }

    public CoreAPIAvailabilityGuard getCoreAPIAvailabilityGuard()
    {
        return coreAPIAvailabilityGuard;
    }
}
