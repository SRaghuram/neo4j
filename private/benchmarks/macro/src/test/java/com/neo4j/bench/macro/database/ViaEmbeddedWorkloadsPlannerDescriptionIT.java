/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.macro.database;

import com.neo4j.bench.common.database.Store;
import com.neo4j.bench.common.tool.macro.Deployment;
import com.neo4j.bench.common.tool.macro.ExecutionMode;
import com.neo4j.bench.macro.StoreTestUtil;
import com.neo4j.bench.macro.database.PlannerDescriptionTestsSupport.EmbeddedPlanDescriptionAccessors;
import com.neo4j.bench.macro.execution.database.EmbeddedDatabase;
import com.neo4j.bench.macro.execution.database.PlannerDescription;
import com.neo4j.bench.macro.workload.Query;
import com.neo4j.bench.macro.workload.Workload;
import com.neo4j.bench.model.model.PlanOperator;
import com.neo4j.bench.model.options.Edition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.bench.macro.database.PlannerDescriptionTestsSupport.assertPlansEqual;
import static java.lang.String.format;

@TestDirectoryExtension
public class ViaEmbeddedWorkloadsPlannerDescriptionIT
{
    private static final Logger LOG = LoggerFactory.getLogger( ViaEmbeddedWorkloadsPlannerDescriptionIT.class );

    @Inject
    public TestDirectory temporaryFolder;
    private final PlannerDescriptionTestsSupport testsSupport = new PlannerDescriptionTestsSupport();

    public static Stream<Arguments> workloads() throws IOException
    {
        return PlannerDescriptionTestsSupport.allWorkloads( Deployment.embedded() ).stream().map( Arguments::of );
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
    public void shouldExtractPlansViaEmbedded( Workload workload )
    {
        Path neo4jConfigFile = testsSupport.writeEmbeddedNeo4jConfig();
        LOG.debug( "Verifying plan extraction on workload: " + workload.name() );
        Path storePath = temporaryFolder.directory( format( "store-%s-%s", workload.name(), testsSupport.randId() ) );
        try ( Store store = StoreTestUtil.createTemporaryEmptyStoreFor( workload, storePath, neo4jConfigFile );
              EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile ) )
        {
            for ( Query query : workload.queries() )
            {
                try
                {
                    try ( var tx = database.inner().beginTx() )
                    {
                        Result result = tx.execute( query.copyWith( ExecutionMode.PLAN ).queryString().value() );
                        result.accept( row -> true );
                        ExecutionPlanDescription rootPlanDescription = result.getExecutionPlanDescription();
                        PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootPlanDescription );
                        assertPlansEqual( rootPlanOperator, rootPlanDescription, new EmbeddedPlanDescriptionAccessors() );
                    }
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
