/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.database;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.Neo4jServerDatabase;
import com.neo4j.bench.macro.execution.database.PlannerDescription;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.options.Edition;
import com.neo4j.common.util.TestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;

import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.Plan;
import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.macro.database.PlannerDescriptionTestsSupport.getJvm;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@TestDirectoryExtension
class PlannerDescriptionIT
{
    private static final String QUERY = "PROFILE MATCH (a:A), (b:B) WHERE a.x = b.x AND b.y='foo' AND (a)--(b) RETURN count(a)";
    private static final String EXPECTED_ASCII_PLAN = "+--------------------+------------------------+----------------+------+\n" +
                                                      "| Operator           | Details                | Estimated Rows | Rows |\n" +
                                                      "+--------------------+------------------------+----------------+------+\n" +
                                                      "| +ProduceResults    | `count(a)`             |              1 |    1 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +EagerAggregation  | count(a) AS `count(a)` |              1 |    1 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +Apply             |                        |              0 |    0 |\n" +
                                                      "| |\\                 +------------------------+----------------+------+\n" +
                                                      "| | +Limit           | 1                      |              0 |    0 |\n" +
                                                      "| | |                +------------------------+----------------+------+\n" +
                                                      "| | +Expand(Into)    | (a)-[anon_56]-(b)      |              0 |    0 |\n" +
                                                      "| | |                +------------------------+----------------+------+\n" +
                                                      "| | +EmptyRow        | a, b                   |              0 |    0 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +ValueHashJoin     | b.x = a.x              |              0 |    0 |\n" +
                                                      "| |\\                 +------------------------+----------------+------+\n" +
                                                      "| | +NodeByLabelScan | a:A                    |             10 |    0 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +Filter            | b.y = $autostring_0    |              0 |    0 |\n" +
                                                      "| |                  +------------------------+----------------+------+\n" +
                                                      "| +NodeByLabelScan   | b:B                    |             10 |    0 |\n" +
                                                      "+--------------------+------------------------+----------------+------+\n";

    private static final String EXPECTED_ASCII_PLAN_SERVER = "+--------------------------+------------------------+----------------+------+\n" +
                                                             "| Operator                 | Details                | Estimated Rows | Rows |\n" +
                                                             "+--------------------------+------------------------+----------------+------+\n" +
                                                             "| +ProduceResults@neo4j    | `count(a)`             |              1 |    1 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +EagerAggregation@neo4j  | count(a) AS `count(a)` |              1 |    1 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +Apply@neo4j             |                        |              0 |    0 |\n" +
                                                             "| |\\                       +------------------------+----------------+------+\n" +
                                                             "| | +Limit@neo4j           | 1                      |              0 |    0 |\n" +
                                                             "| | |                      +------------------------+----------------+------+\n" +
                                                             "| | +Expand(Into)@neo4j    | (a)-[anon_56]-(b)      |              0 |    0 |\n" +
                                                             "| | |                      +------------------------+----------------+------+\n" +
                                                             "| | +Argument@neo4j        | a, b                   |              0 |    0 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +ValueHashJoin@neo4j     | b.x = a.x              |              0 |    0 |\n" +
                                                             "| |\\                       +------------------------+----------------+------+\n" +
                                                             "| | +NodeByLabelScan@neo4j | a:A                    |             10 |    0 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +Filter@neo4j            | b.y = $autostring_0    |              0 |    0 |\n" +
                                                             "| |                        +------------------------+----------------+------+\n" +
                                                             "| +NodeByLabelScan@neo4j   | b:B                    |             10 |    0 |\n" +
                                                             "+--------------------------+------------------------+----------------+------+\n";

    @Inject
    private TestDirectory temporaryFolder;
    private final PlannerDescriptionTestsSupport testsSupport = new PlannerDescriptionTestsSupport();

    @BeforeEach
    public void copyNeo4j() throws Exception
    {
        testsSupport.copyNeo4jDir( temporaryFolder );
    }

    @AfterEach
    public void deleteTemporaryNeo4j() throws IOException
    {
        testsSupport.deleteTemporaryNeo4j();
    }

    @Test
    public void shouldRenderAsciiPlanFromEmbedded()
    {
        Path neo4jConfigFile = testsSupport.writeEmbeddedNeo4jConfig();
        try ( Store store = TestSupport.createTemporaryEmptyStore( temporaryFolder.directory( format( "store-%s", testsSupport.randId() ) ), neo4jConfigFile );
              EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile );
              org.neo4j.graphdb.Transaction tx = database.inner().beginTx() )
        {
            Result result = tx.execute( QUERY );
            result.accept( row -> true );
            ExecutionPlanDescription rootPlanDescription = result.getExecutionPlanDescription();
            PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootPlanDescription );
            String asciiPlan = PlannerDescription.toAsciiPlan( rootPlanOperator );
            assertThat( asciiPlan, equalTo( EXPECTED_ASCII_PLAN ) );
        }
    }

    @Test
    public void shouldRenderAsciiPlanFromDriver() throws Exception
    {
        Jvm jvm = getJvm();
        Path neo4jConfigFile = testsSupport.writeServerNeo4jConfig( "ascii" );

        Redirect outputRedirect = Redirect.to( temporaryFolder.file( format( "neo4j-out-%s.log", testsSupport.randId() ) ).toFile() );
        Redirect errorRedirect = Redirect.to( temporaryFolder.file( format( "neo4j-error-%s.log", testsSupport.randId() ) ).toFile() );
        Path logsDir = Files.createDirectories( temporaryFolder.directory( format( "logs-%s", testsSupport.randId() ) ) );

        try ( Store store = TestSupport.createTemporaryEmptyStore( temporaryFolder.directory( format( "store-%s", testsSupport.randId() ) ), neo4jConfigFile );
              ServerDatabase database = Neo4jServerDatabase.startServer( jvm,
                                                                         testsSupport.neo4jDir(),
                                                                         store,
                                                                         neo4jConfigFile,
                                                                         outputRedirect,
                                                                         errorRedirect,
                                                                         logsDir );
              Transaction tx = database.session().beginTransaction() )
        {
            org.neo4j.driver.Result result = tx.run( QUERY );
            Plan rootDriverPlan = result.consume().plan();
            PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootDriverPlan );
            String asciiPlan = PlannerDescription.toAsciiPlan( rootPlanOperator );
            assertThat( asciiPlan, equalTo( EXPECTED_ASCII_PLAN_SERVER ) );
        }
    }

}
