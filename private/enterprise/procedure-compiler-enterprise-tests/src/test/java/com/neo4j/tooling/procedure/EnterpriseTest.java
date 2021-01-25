/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tooling.procedure;

import com.google.testing.compile.CompilationRule;
import com.google.testing.compile.CompileTester;
import com.google.testing.compile.JavaFileObjects;
import org.junit.Rule;
import org.junit.Test;

import java.net.URL;
import javax.annotation.processing.Processor;
import javax.tools.JavaFileObject;

import org.neo4j.tooling.procedure.ProcedureProcessor;

import static com.google.common.truth.Truth.assert_;
import static com.google.testing.compile.JavaSourceSubjectFactory.javaSource;

public class EnterpriseTest
{

    @Rule
    public CompilationRule compilation = new CompilationRule();

    private Processor processor = new ProcedureProcessor();

    @Test
    public void emits_warnings_for_restricted_enterprise_types()
    {

        JavaFileObject sproc =
                JavaFileObjects.forResource( resolveUrl( "context/restricted_types/EnterpriseProcedure.java" ) );

        CompileTester.SuccessfulCompilationClause warningCompilationClause =
                assert_().about( javaSource() ).that( sproc ).processedWith( processor ).compilesWithoutError()
                        .withWarningCount( 3 );
        warningCompilationClause.withWarningContaining(
                "@org.neo4j.procedure.Context usage warning: found unsupported restricted type " +
                "<com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager> on EnterpriseProcedure#enterpriseAuthManager.\n" +
                "  The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n" +
                "  You can ignore this warning by passing the option -AIgnoreContextWarnings to the Java compiler" )
                .in( sproc ).onLine( 23 );
        warningCompilationClause.withWarningContaining(
                "@org.neo4j.procedure.Context usage warning: found unsupported restricted type " +
                "<com.neo4j.server.security.enterprise.log.SecurityLog> on EnterpriseProcedure#securityLog.\n" +
                "  The procedure will not load unless declared via the configuration option 'dbms.security.procedures.unrestricted'.\n" +
                "  You can ignore this warning by passing the option -AIgnoreContextWarnings to the Java compiler" )
                .in( sproc ).onLine( 26 );
    }

    @Test
    public void does_not_emit_warnings_for_restricted_enterprise_types_when_warnings_are_disabled()
    {

        JavaFileObject sproc =
                JavaFileObjects.forResource( resolveUrl( "context/restricted_types/EnterpriseProcedure.java" ) );

        assert_().about( javaSource() ).that( sproc ).withCompilerOptions( "-AIgnoreContextWarnings" )
                .processedWith( processor ).compilesWithoutError().withWarningCount( 1 );
    }

    private URL resolveUrl( String relativePath )
    {
        return this.getClass().getResource( "/com/neo4j/tooling/procedure/procedures/" + relativePath );
    }
}
