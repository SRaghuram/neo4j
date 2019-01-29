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

package com.neo4j.bench.ldbc.interactive.integration;

import com.ldbc.driver.DbException;
import com.neo4j.bench.ldbc.connection.CsvSchema;
import com.neo4j.bench.ldbc.connection.Neo4jApi;
import com.neo4j.bench.ldbc.connection.Neo4jSchema;
import com.neo4j.bench.ldbc.importer.Scenario;
import com.neo4j.bench.ldbc.utils.PlannerType;
import com.neo4j.bench.ldbc.utils.RuntimeType;

public class EmbeddedCoreRegularSnbInteractiveExecutionTest extends SnbInteractiveExecutionTest
{
    @Override
    Scenario buildValidationData() throws DbException
    {
        return Scenario.randomInteractiveFor(
                CsvSchema.CSV_REGULAR,
                Neo4jSchema.NEO4J_REGULAR,
                Scenario.randomNeo4jImporterFor( CsvSchema.CSV_REGULAR, Neo4jSchema.NEO4J_REGULAR ),
                Neo4jApi.EMBEDDED_CORE,
                PlannerType.DEFAULT,
                RuntimeType.DEFAULT );
    }
}
