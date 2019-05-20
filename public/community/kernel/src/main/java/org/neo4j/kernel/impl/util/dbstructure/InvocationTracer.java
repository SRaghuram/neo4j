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
package org.neo4j.kernel.impl.util.dbstructure;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.neo4j.internal.helpers.collection.Visitable;

public class InvocationTracer<C> implements InvocationHandler, AutoCloseable
{
    private final String generatorInfo;

    private final String generatedClassPackage;
    private final String generatedClassName;

    private final Class<C> interfaceClass;
    private final ArgumentFormatter argumentFormatter;
    private final Appendable output;

    private boolean open = true;

    public InvocationTracer( String generatorInfo,
                             String generatedClassPackage,
                             String generatedClassName,
                             Class<C> interfaceClass,
                             ArgumentFormatter argumentFormatter,
                             Appendable output )
            throws IOException
    {
        this.generatorInfo = generatorInfo;

        if ( generatedClassName.contains( "." ) || generatedClassName.contains( "%" ) )
        {
            throw new IllegalArgumentException( "Invalid class name: " + generatedClassName );
        }

        if ( generatedClassPackage.contains( "%" ) )
        {
            throw new IllegalArgumentException( "Invalid class package: " + generatedClassPackage );
        }

        this.generatedClassPackage = generatedClassPackage;
        this.generatedClassName = generatedClassName;
        this.interfaceClass = interfaceClass;
        this.argumentFormatter = argumentFormatter;
        this.output = output;

        formatPreamble( output );
    }

    public C newProxy()
    {
        return newProxy( interfaceClass );
    }

    public <P extends C> P newProxy( Class<P> proxyClass )
    {
        ClassLoader classLoader = proxyClass.getClassLoader();
        return proxyClass.cast(
            Proxy.newProxyInstance( classLoader, new Class[]{proxyClass}, this )
        );
    }

    @Override
    public void close() throws IOException
    {
        if ( open )
        {
            formatAppendix( output );
            open = false;
        }
        else
        {
            throw new IllegalStateException( "Already closed" );
        }
    }

    private void formatPreamble( Appendable builder ) throws IOException
    {
        String interfaceSimpleName = interfaceClass.getSimpleName();
        String interfaceClassName =
            interfaceSimpleName.length() == 0 ? interfaceClass.getCanonicalName() : interfaceSimpleName;
        if ( generatedClassPackage.length() > 0 )
        {
            formatln( builder, "package %s;", generatedClassPackage );
            formatln( builder );
        }
        formatln( builder, "import %s;", Visitable.class.getCanonicalName() );
        formatln( builder, "import %s;", interfaceClass.getCanonicalName() );
        formatln( builder );
        for ( String importExpr : argumentFormatter.imports() )
        {
            formatln( builder, "import %s;", importExpr );
        }
        formatln( builder );
        formatln( builder, "//" );
        formatln( builder, "// GENERATED FILE. DO NOT EDIT." );
        formatln( builder, "//" );
        formatln( builder, "// This has been generated by:" );
        formatln( builder, "//" );
        if ( generatorInfo.length() > 0 )
        {
            formatln( builder, "//   %s", generatorInfo );
            formatln( builder, "//" );
            formatln( builder, "// (using %s)", getClass().getCanonicalName() );
            formatln( builder, "//" );
        }
        else
        {
            formatln( builder, "//   %s", getClass().getCanonicalName() );
            formatln( builder, "//" );
        }
        formatln( builder );
        formatln( builder, "public enum %s", generatedClassName );
        formatln( builder, "implements %s<%s>", Visitable.class.getSimpleName(), interfaceClassName );
        formatln( builder, "{" );
        formatln( builder, "    INSTANCE;" );
        formatln( builder );
        formatln( builder, "    public void accept( %s visitor )", interfaceClassName );
        formatln( builder, "    {" );
    }

    @Override
    public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
    {
        if ( open )
        {
            if ( method.getReturnType().equals( Void.TYPE ) )
            {
                // formatln invocation start
                format( output, "        visitor.%s(", method.getName() );

                // formatln arguments
                for ( int i = 0; i < args.length; i++ )
                {
                    Object arg = args[i];

                    if ( i > 0 )
                    {
                        format( output, ", " );
                    }
                    else
                    {
                        format( output, " " );
                    }

                    argumentFormatter.formatArgument( output, arg );
                }

                // formatln invocation end
                if ( args.length == 0 )
                {
                    formatln( output, ");" );
                }
                else
                {
                    formatln( output, " );" );
                }

                return null;
            }
            else
            {
                throw new IllegalArgumentException( "InvocationTraceGenerator only works with void methods" );
            }
        }
        else
        {
            throw new IllegalStateException( "Tracer already closed" );
        }
    }

    private static void formatAppendix( Appendable builder ) throws IOException
    {
        formatln( builder, "   }" );
        formatln( builder, "}" );
        formatln( builder );
        formatln( builder, "/* END OF GENERATED CONTENT */" );
    }

    private static void formatln( Appendable output, String format, Object... args ) throws IOException
    {
        format( output, format, args );
        formatln( output );
    }

    private static void format( Appendable output, String format, Object... args ) throws IOException
    {
        output.append( String.format( format, args ) );
    }

    private static void formatln( Appendable output ) throws IOException
    {
        output.append( System.lineSeparator() );
    }
}
