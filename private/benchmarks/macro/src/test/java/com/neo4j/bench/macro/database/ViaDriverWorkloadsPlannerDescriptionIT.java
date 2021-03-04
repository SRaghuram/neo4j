/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.database;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.macro.StoreTestUtil;
import com.neo4j.bench.macro.execution.database.Neo4jServerDatabase;
import com.neo4j.bench.macro.execution.database.PlannerDescription;
import com.neo4j.bench.macro.execution.database.ServerDatabase;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.PlanOperator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.summary.Plan;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.macro.database.PlannerDescriptionTestsSupport.assertPlansEqual;
import static com.neo4j.bench.macro.database.PlannerDescriptionTestsSupport.getJvm;
import static java.lang.String.format;

@TestDirectoryExtension
public class ViaDriverWorkloadsPlannerDescriptionIT
{
    private static final Logger LOG = LoggerFactory.getLogger( ViaDriverWorkloadsPlannerDescriptionIT.class );

    @Inject
    public TestDirectory temporaryFolder;
    private final PlannerDescriptionTestsSupport testsSupport = new PlannerDescriptionTestsSupport();

    public static Stream<Arguments> workloads() throws IOException
    {
        return PlannerDescriptionTestsSupport.allWorkloads( Deployment.server() ).stream().map( Arguments::of );
    }

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

    @ParameterizedTest
    @MethodSource( {"workloads"} )
    public void shouldExtractPlansViaDriver( Workload workload ) throws IOException, TimeoutException
    {
        Jvm jvm = getJvm();

        LOG.debug( "Verifying plan extraction on workload: " + workload.name() );
        ProcessBuilder.Redirect
                outputRedirect =
                ProcessBuilder.Redirect.to( temporaryFolder.file( format( "neo4j-out-%s-%s.log", workload.name(), testsSupport.randId() ) ) );
        ProcessBuilder.Redirect
                errorRedirect =
                ProcessBuilder.Redirect.to( temporaryFolder.file( format( "neo4j-error-%s-%s.log", workload.name(), testsSupport.randId() ) ) );
        Path logsDir = Files.createDirectories( temporaryFolder.directory( format( "logs-%s-%s", workload.name(), testsSupport.randId() ) ).toPath() );
        Path neo4jConfigFile = testsSupport.writeServerNeo4jConfig( workload.name() );
        Path storePath = temporaryFolder.directory( format( "store-%s-%s", workload.name(), testsSupport.randId() ) ).toPath();
        try ( Store store = StoreTestUtil.createEmptyStoreFor( workload, storePath, neo4jConfigFile );
              ServerDatabase database = Neo4jServerDatabase.startServer( jvm,
                                                                         testsSupport.neo4jDir(),
                                                                         store,
                                                                         neo4jConfigFile,
                                                                         outputRedirect,
                                                                         errorRedirect,
                                                                         logsDir ) )
        {
            for ( Query query : workload.queries() )
            {
                try ( Transaction tx = database.session().beginTransaction() )
                {
                    Result result = tx.run( query.copyWith( ExecutionMode.PLAN ).queryString().value() );
                    Plan rootDriverPlan = result.consume().plan();
                    PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootDriverPlan );
                    assertPlansEqual( rootPlanOperator, rootDriverPlan, new PlannerDescriptionTestsSupport.DriverPlanAccessors() );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( format( "Plans comparison failed!\n" +
                                                        "Workload: %s\n" +
                                                        "Query:    %s", workload.name(), query.name() ), e );
                }
            }
        }
    }
}
