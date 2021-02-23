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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;

import org.neo4j.graphdb.ExecutionPlanDescription;
import org.neo4j.graphdb.Result;

import static com.neo4j.bench.macro.database.PlannerDescriptionTestsSupport.assertPlansEqual;
import static java.lang.String.format;

@RunWith( Parameterized.class )
public class ViaEmbeddedWorkloadsPlannerDescriptionIT
{
    private static final Logger LOG = LoggerFactory.getLogger( ViaEmbeddedWorkloadsPlannerDescriptionIT.class );

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final PlannerDescriptionTestsSupport testsSupport = new PlannerDescriptionTestsSupport();

    private final Workload workload;

    @Parameters
    public static Collection<Object[]> workloads() throws IOException
    {
        return PlannerDescriptionTestsSupport.allWorkloads( Deployment.embedded() );
    }

    public ViaEmbeddedWorkloadsPlannerDescriptionIT( Workload workload )
    {
        this.workload = workload;
    }

    @Before
    public void copyNeo4j() throws Exception
    {
        testsSupport.copyNeo4jDir( temporaryFolder );
    }

    @After
    public void deleteTemporaryNeo4j() throws IOException
    {
        testsSupport.deleteTemporaryNeo4j();
    }

    @Test
    public void shouldExtractPlansViaEmbedded() throws IOException
    {
        Path neo4jConfigFile = testsSupport.writeEmbeddedNeo4jConfig();
        LOG.debug( "Verifying plan extraction on workload: " + workload.name() );
        Path storePath = temporaryFolder.newFolder( format( "store-%s-%s", workload.name(), testsSupport.randId() ) ).toPath();
        try ( Store store = StoreTestUtil.createTemporaryEmptyStoreFor( workload, storePath, neo4jConfigFile );
              EmbeddedDatabase database = EmbeddedDatabase.startWith( store, Edition.ENTERPRISE, neo4jConfigFile ) )
        {
            for ( Query query : workload.queries() )
            {
                try
                {
                    Result result = database.inner().execute( query.copyWith( ExecutionMode.PLAN ).queryString().value() );
                    result.accept( row -> true );
                    ExecutionPlanDescription rootPlanDescription = result.getExecutionPlanDescription();
                    PlanOperator rootPlanOperator = PlannerDescription.toPlanOperator( rootPlanDescription );
                    assertPlansEqual( rootPlanOperator, rootPlanDescription, new EmbeddedPlanDescriptionAccessors() );
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
