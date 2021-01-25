/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.process;

import com.neo4j.bench.common.util.BenchmarkUtil;
import com.neo4j.bench.common.util.Jvm;
import com.neo4j.bench.model.process.JvmArgs;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JvmProcessArgs
{
    private final List<String> jvmInvokeArg;
    private final Jvm jvm;
    private final JvmArgs jvmArgs;
    private final List<String> toolCommandArgs;
    private final String classPath;
    private final Class<?> mainClass;
    private final String processName;

    /**
     * Creates commandline args for launching a child benchmark process
     *
     * @param jvmInvokeArgs prefix java invoke options, e.g., if any profiler wants it
     * @param jvm the JVM run fork with
     * @param jvmArgs jvm args to run fork with
     * @param toolCommandArgs arguments of command specified by {@code toolCommand}
     * @return commands that can be passed to a {@link java.lang.ProcessBuilder}
     */
    public static JvmProcessArgs argsForJvmProcess( List<String> jvmInvokeArgs,
                                                    Jvm jvm,
                                                    JvmArgs jvmArgs,
                                                    List<String> toolCommandArgs,
                                                    Class<?> mainClass )
    {
        return new JvmProcessArgs( jvmInvokeArgs, jvm, jvmArgs, toolCommandArgs, System.getProperty( "java.class.path" ), mainClass );
    }

    private JvmProcessArgs( List<String> jvmInvokeArgs,
                            Jvm jvm,
                            JvmArgs jvmArgs,
                            List<String> toolCommandArgs,
                            String classPath,
                            Class<?> mainClass )
    {
        this.processName = BenchmarkUtil.sanitize( UUID.randomUUID().toString() );
        this.jvmInvokeArg = jvmInvokeArgs;
        this.jvm = jvm;
        this.jvmArgs = JvmArgs
                .from( "-Dname=" + processName )
                .merge( jvmArgs );
        this.toolCommandArgs = toolCommandArgs;
        this.classPath = classPath;
        this.mainClass = mainClass;
    }

    String processName()
    {
        return processName;
    }

    public Jvm jvm()
    {
        return jvm;
    }

    List<String> args()
    {
        List<String> commands = new ArrayList<>();
        commands.addAll( jvmInvokeArg );
        commands.add( jvm.launchJava() );
        commands.addAll( jvmArgs.toArgs() );
        commands.add( "-cp" );
        commands.add( classPath );
        commands.add( mainClass.getName() );
        commands.addAll( toolCommandArgs );
        return commands;
    }

    String conciseToString()
    {
        return "ProcessArgs\n" +
               "\tjvmInvokeArg    : " + jvmInvokeArg + "\n" +
               "\tjvm             : " + jvm + "\n" +
               "\tjvmArgs         : " + jvmArgs + "\n" +
               "\ttoolCommandArgs : " + toolCommandArgs + "\n" +
               "\tclassPath       : <omitted>\n" +
               "\tmainClass       : " + mainClass + "\n" +
               "\tprocessName     : " + processName;
    }

    @Override
    public String toString()
    {
        return "ProcessArgs\n" +
               "\tjvmInvokeArg    : " + jvmInvokeArg + "\n" +
               "\tjvm             : " + jvm + "\n" +
               "\tjvmArgs         : " + jvmArgs + "\n" +
               "\ttoolCommandArgs : " + toolCommandArgs + "\n" +
               "\tclassPath       : " + classPath + "\n" +
               "\tmainClass       : " + mainClass + "\n" +
               "\tprocessName     : " + processName + "\n" +
               "\tALL             : " + args();
    }
}
