/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.jmh.api.config;

import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;

public class AnnotationsValidator
{
    private final Annotations annotations;

    AnnotationsValidator( Annotations annotations )
    {
        this.annotations = annotations;
    }

    AnnotationsValidationResult validate()
    {
        boolean valid = hasAtLeastOneBenchmarkClass() &&
                        hasClassNamePrefixOnAllParamFieldNames() &&
                        hasEmptyValueOnAllParamFields() &&
                        hasParamValueOnAllParamFields() &&
                        hasParamOnAllParamValueFields() &&
                        hasNonEmptyAllowedOnAllParamValueFields() &&
                        hasNonEmptyBaseOnAllParamValueFields() &&
                        isBaseValuesSubsetsOfAllowed() &&
                        noBenchmarkHasInvalidSetup() &&
                        noBenchmarkHasInvalidTearDown() &&
                        allBenchmarkMethodsHaveModeAnnotation();
        String validationMessage = valid ? "Validation Passed" : validationErrors();
        return new AnnotationsValidationResult( valid, validationMessage );
    }

    private boolean hasAtLeastOneBenchmarkClass()
    {
        // Sanity check that reflection code finds benchmark classes, if it finds one class it probably finds them all
        return annotations.getBenchmarkMethods().keySet().size() > 0;
    }

    private boolean hasClassNamePrefixOnAllParamFieldNames()
    {
        // JMH options builder does not allow for setting param:value pairs on individual classes, only globally
        // To get around that this benchmark suite prepends benchmark class names onto the param names of those classes
        // This test makes sure this policy is being followed
        return annotations.getParamFieldsWithoutClassNamePrefix().size() == 0;
    }

    private boolean hasEmptyValueOnAllParamFields()
    {
        // JMH options builder does not allow for setting param:value pairs on individual classes, only globally
        // To get around that this benchmark suite prepends benchmark class names onto the param names of those classes
        // This test makes sure this policy is being followed
        return annotations.assignedParamFields().size() == 0;
    }

    private boolean hasParamValueOnAllParamFields()
    {
        return annotations.paramFieldsWithoutParamValue().size() == 0;
    }

    private boolean hasParamOnAllParamValueFields()
    {
        return annotations.paramValueFieldsWithoutParam().size() == 0;
    }

    private boolean hasNonEmptyAllowedOnAllParamValueFields()
    {
        return annotations.paramValueFieldsWithEmptyAllowed().size() == 0;
    }

    private boolean allBenchmarksExtendBaseBenchmark()
    {
        return annotations.classesThatDoNotExtendBaseBenchmark().isEmpty();
    }

    private boolean hasNonEmptyBaseOnAllParamValueFields()
    {
        return annotations.paramValueFieldsWithEmptyBase().size() == 0;
    }

    private boolean isBaseValuesSubsetsOfAllowed()
    {
        return annotations.erroneousBaseValues().isEmpty();
    }

    private boolean noBenchmarkHasInvalidSetup()
    {
        return annotations.classesWithSetupMethod().isEmpty();
    }

    private boolean noBenchmarkHasInvalidTearDown()
    {
        return annotations.classesWithTearDownMethod().isEmpty();
    }

    private boolean allBenchmarkMethodsHaveModeAnnotation()
    {
        return annotations.benchmarkMethodsWithoutModeAnnotation().isEmpty();
    }

    private String validationErrors()
    {
        final StringBuilder sb = new StringBuilder( "Validation Failed\n" );
        if ( !hasAtLeastOneBenchmarkClass() )
        {
            sb.append( "\t" ).append( "* No benchmark classes found\n" );
        }
        if ( !hasClassNamePrefixOnAllParamFieldNames() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( Param.class.getSimpleName() )
                    .append( " fields lack class name prefix:\n" );
            annotations.getParamFieldsWithoutClassNamePrefix().forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getName() )
                    .append( " in " ).append( field.getDeclaringClass().getName() )
                    .append( " should have prefix: " ).append( field.getDeclaringClass().getSimpleName() ).append( "_" )
                    .append( "\n" )
            );
        }
        if ( !hasEmptyValueOnAllParamFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( Param.class.getSimpleName() )
                    .append( " annotations should have no value, but these fields do:\n" );
            annotations.assignedParamFields().forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasParamValueOnAllParamFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( Param.class.getSimpleName() )
                    .append( " fields should also have @" ).append( ParamValues.class.getSimpleName() )
                    .append( " annotation. These do not:\n" );
            annotations.paramFieldsWithoutParamValue().forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasParamOnAllParamValueFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " fields should have @" ).append( Param.class.getSimpleName() )
                    .append( " annotation. These do not:\n" );
            annotations.paramValueFieldsWithoutParam().forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasNonEmptyAllowedOnAllParamValueFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " annotations should have non-empty 'allowed' field. These do not:\n" );
            annotations.paramValueFieldsWithEmptyAllowed().forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !hasNonEmptyBaseOnAllParamValueFields() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " annotations should have non-empty 'base' field. These do not:\n" );
            annotations.paramValueFieldsWithEmptyBase().forEach( field -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( field.getDeclaringClass().getName() ).append( "." ).append( field.getName() )
                    .append( "\n" )
            );
        }
        if ( !isBaseValuesSubsetsOfAllowed() )
        {
            sb
                    .append( "\t" ).append( "* @" ).append( ParamValues.class.getSimpleName() )
                    .append( " 'base' & 'extended' value must be subset of 'allowed'. These are not:\n" );
            annotations.erroneousBaseValues().entrySet().forEach( entry -> sb
                    .append( "\t\t" ).append( "> " )
                    .append( entry.getKey().getDeclaringClass().getName() ).append( "." )
                    .append( entry.getKey().getName() )
                    .append( " has illegal 'base' values " ).append( entry.getValue() )
                    .append( "\n" )
            );
        }
        if ( !allBenchmarksExtendBaseBenchmark() )
        {
            sb
                    .append( "\t" ).append( "* Classes should extend base benchmarks. These classes do not:\n" );
            annotations.classesThatDoNotExtendBaseBenchmark().forEach( clazz -> sb
                    .append( "\t\t" ).append( "> " ).append( clazz.getName() ).append( "\n" )
            );
        }
        if ( !noBenchmarkHasInvalidSetup() )
        {
            sb
                    .append( "\t" ).append( "* Benchmarks should not have methods with @" )
                    .append( Setup.class.getName() ).append( "(" ).append( Level.Trial ).append( "), but these benchmarks do: \n " );
            annotations.classesWithSetupMethod().forEach( benchmark -> sb
                    .append( "\t\t" ).append( "> " ).append( benchmark.getName() ).append( "\n" )
            );
        }
        if ( !noBenchmarkHasInvalidTearDown() )
        {
            sb
                    .append( "\t" ).append( "* Benchmarks should not have methods with @" )
                    .append( TearDown.class.getName() ).append( "(" ).append( Level.Trial ).append( "), but these benchmarks do: \n " );
            annotations.classesWithTearDownMethod().forEach( benchmark -> sb
                    .append( "\t\t" ).append( "> " ).append( benchmark.getName() ).append( "\n" )
            );
        }
        if ( !allBenchmarkMethodsHaveModeAnnotation() )
        {
            sb
                    .append( "\t" ).append( "* Benchmark methods should be annotated with " )
                    .append( BenchmarkMode.class.getName() ).append( ", these methods do not:\n" );
            annotations.benchmarkMethodsWithoutModeAnnotation().forEach( method -> sb
                    .append( "\t\t" ).append( "> " ).append( method.getDeclaringClass().getName() ).append( "." )
                    .append( method.getName() ).append( "\n" )
            );
        }
        return sb.toString();
    }

    public static class AnnotationsValidationResult
    {
        private final boolean valid;
        private final String message;

        AnnotationsValidationResult( boolean valid, String message )
        {
            this.valid = valid;
            this.message = message;
        }

        public boolean isValid()
        {
            return valid;
        }

        public String message()
        {
            return message;
        }

        public static void assertValid( AnnotationsValidationResult annotationsValidationResult )
        {
            if ( !annotationsValidationResult.isValid() )
            {
                throw new RuntimeException( annotationsValidationResult.message() );
            }
        }
    }
}
