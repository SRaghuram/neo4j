/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.interactive.embedded_core_regular;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.neo4j.bench.ldbc.Retries;
import com.neo4j.bench.ldbc.connection.Neo4jConnectionState;
import com.neo4j.bench.ldbc.interactive.Neo4jUpdate1;
import com.neo4j.bench.ldbc.interactive.embedded_core.Update1EmbeddedCore_0;

import static com.neo4j.bench.ldbc.Retries.runInTransaction;

public class Update1HandlerEmbeddedCoreRegular implements OperationHandler<LdbcUpdate1AddPerson,Neo4jConnectionState>
{
    private static final Neo4jUpdate1<Neo4jConnectionState> QUERY = new Update1EmbeddedCore_0();

    @Override
    public void executeOperation( LdbcUpdate1AddPerson operation, Neo4jConnectionState dbConnectionState,
            ResultReporter resultReporter ) throws DbException
    {
        Retries<LdbcUpdate1AddPerson,LdbcNoResult> retries = runInTransaction( QUERY, operation, dbConnectionState );
        resultReporter.report( retries.encodedResultsCode(), retries.result(), operation );
    }
}
