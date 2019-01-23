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
package org.neo4j.kernel.impl.proc;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.common.DependencyResolver;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.Neo4jTypes;
import org.neo4j.kernel.api.ResourceTracker;
import org.neo4j.kernel.api.StubResourceManager;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.values.ValueMapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.helpers.collection.Iterators.asList;
import static org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature;
import static org.neo4j.kernel.api.proc.BasicContext.buildContext;

@SuppressWarnings( "WeakerAccess" )
public class ReflectiveProcedureWithArgumentsTest
{
    private final ResourceTracker resourceTracker = new StubResourceManager();
    private final DependencyResolver dependencyResolver = new Dependencies();
    private final ValueMapper<Object> valueMapper = new DefaultValueMapper( mock( EmbeddedProxySPI.class ) );

    @Test
    void shouldCompileSimpleProcedure() throws Throwable
    {
        // When
        List<CallableProcedure> procedures = compile( ClassWithProcedureWithSimpleArgs.class );

        // Then
        assertEquals( 1, procedures.size() );
        assertThat( procedures.get( 0 ).signature(), equalTo(
                procedureSignature( "org", "neo4j", "kernel", "impl", "proc", "listCoolPeople" )
                        .in( "name", Neo4jTypes.NTString )
                        .in( "age", Neo4jTypes.NTInteger )
                        .out( "name", Neo4jTypes.NTString )
                        .build() ) );
    }

    @Test
    void shouldRunSimpleProcedure() throws Throwable
    {
        // Given
        CallableProcedure procedure = compile( ClassWithProcedureWithSimpleArgs.class ).get( 0 );

        // When
        RawIterator<Object[],ProcedureException> out = procedure.apply( prepareContext(), new Object[]{"Pontus", 35L}, resourceTracker );

        // Then
        List<Object[]> collect = asList( out );
        assertThat( collect.get( 0 )[0], equalTo( "Pontus is 35 years old." ) );
    }

    @Test
    void shouldRunGenericProcedure() throws Throwable
    {
        // Given
        CallableProcedure procedure = compile( ClassWithProcedureWithGenericArgs.class ).get( 0 );

        // When
        RawIterator<Object[],ProcedureException> out = procedure.apply( prepareContext(), new Object[]{
                Arrays.asList( "Roland", "Eddie", "Susan", "Jake" ),
                Arrays.asList( 1000L, 23L, 29L, 12L )}, resourceTracker );

        // Then
        List<Object[]> collect = asList( out );
        assertThat( collect.get( 0 )[0], equalTo( "Roland is 1000 years old." ) );
        assertThat( collect.get( 1 )[0], equalTo( "Eddie is 23 years old." ) );
        assertThat( collect.get( 2 )[0], equalTo( "Susan is 29 years old." ) );
        assertThat( collect.get( 3 )[0], equalTo( "Jake is 12 years old." ) );
    }

    @Test
    void shouldFailIfMissingAnnotations()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( ClassWithProcedureWithoutAnnotatedArgs.class ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Argument at position 0 in method `listCoolPeople` " +
                                                    "is missing an `@Name` annotation.%n" +
                                                    "Please add the annotation, recompile the class and try again." ) ) );
    }

    @Test
    void shouldFailIfMisplacedDefaultValue()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( ClassWithProcedureWithMisplacedDefault.class ) );
        assertThat( exception.getMessage(), containsString(
                "Non-default argument at position 2 with name c in method defaultValues follows default argument. " +
                "Add a default value or rearrange arguments so that the non-default values comes first." ) );
    }

    @Test
    void shouldFailIfWronglyTypedDefaultValue()
    {
        ProcedureException exception = assertThrows( ProcedureException.class, () -> compile( ClassWithProcedureWithBadlyTypedDefault.class ) );
        assertThat( exception.getMessage(), equalTo( String.format( "Argument `a` at position 0 in `defaultValues` with%n" +
                "type `long` cannot be converted to a Neo4j type: Default value `forty-two` could not be parsed as a Long" ) ) );
    }

    private org.neo4j.kernel.api.proc.Context prepareContext()
    {
        return buildContext( dependencyResolver, valueMapper ).context();
    }

    public static class MyOutputRecord
    {
        public String name;

        public MyOutputRecord( String name )
        {
            this.name = name;
        }
    }

    public static class ClassWithProcedureWithSimpleArgs
    {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople( @Name( "name" ) String name, @Name( "age" ) long age )
        {
            return Stream.of( new MyOutputRecord( name + " is " + age + " years old." ) );
        }
    }

    public static class ClassWithProcedureWithGenericArgs
    {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople( @Name( "names" ) List<String> names,
                                                      @Name( "age" ) List<Long> ages )
        {
            Iterator<String> nameIterator = names.iterator();
            Iterator<Long> ageIterator = ages.iterator();
            List<MyOutputRecord> result = new ArrayList<>( names.size() );
            while ( nameIterator.hasNext() )
            {
                long age = ageIterator.hasNext() ? ageIterator.next() : -1;
                result.add( new MyOutputRecord( nameIterator.next() + " is " + age + " years old." ) );
            }
            return result.stream();
        }
    }

    public static class ClassWithProcedureWithoutAnnotatedArgs
    {
        @Procedure
        public Stream<MyOutputRecord> listCoolPeople( String name, int age )
        {
            return Stream.of( new MyOutputRecord( name + " is " + age + " years old." ) );
        }
    }

    public static class ClassWithProcedureWithDefaults
    {
        @Procedure
        public Stream<MyOutputRecord> defaultValues( @Name( value = "a", defaultValue = "a" ) String a,
                @Name( value = "b", defaultValue = "42" ) long b, @Name( value = "c", defaultValue = "3.14" ) double c )
        {
            return Stream.empty();
        }
    }

    public static class ClassWithProcedureWithMisplacedDefault
    {
        @Procedure
        public Stream<MyOutputRecord> defaultValues( @Name( "a" ) String a,
                @Name( value = "b", defaultValue = "42" ) long b, @Name( "c" ) Object c )
        {
            return Stream.empty();
        }
    }

    public static class ClassWithProcedureWithBadlyTypedDefault
    {
        @Procedure
        public Stream<MyOutputRecord> defaultValues( @Name( value = "a", defaultValue = "forty-two" ) long b )
        {
            return Stream.empty();
        }
    }

    private List<CallableProcedure> compile( Class<?> clazz ) throws KernelException
    {
        return new ReflectiveProcedureCompiler( new TypeMappers(), new ComponentRegistry(), new ComponentRegistry(),
                NullLog.getInstance(), ProcedureConfig.DEFAULT ).compileProcedure( clazz, null, true );
    }
}
