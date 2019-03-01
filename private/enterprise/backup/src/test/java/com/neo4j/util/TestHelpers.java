/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.util;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.StreamConsumer;

import static org.neo4j.test.proc.ProcessUtil.getClassPath;
import static org.neo4j.test.proc.ProcessUtil.getJavaExecutable;

public class TestHelpers
{
    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, String... args ) throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( neo4jHome, System.out, System.err, true, args );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, PrintStream outPrintStream, PrintStream errPrintStream,
            boolean debug, String... args ) throws Exception
    {
        List<String> allArgs =
                new ArrayList<>( Arrays.asList( getJavaExecutable().toString(), "-cp", getClassPath(), AdminTool.class.getName() ) );
        allArgs.add( "backup" );
        allArgs.addAll( Arrays.asList( args ) );

        ProcessBuilder processBuilder = new ProcessBuilder().command( allArgs.toArray( new String[0] ));
        processBuilder.environment().put( "NEO4J_HOME", neo4jHome.getAbsolutePath() );
        if ( debug )
        {
            processBuilder.environment().put( "NEO4J_DEBUG", "anything_works" );
        }
        Process process = processBuilder.start();
        ProcessStreamHandler processStreamHandler =
                new ProcessStreamHandler( process, false, "", StreamConsumer.IGNORE_FAILURES, outPrintStream, errPrintStream );
        return processStreamHandler.waitForResult();
    }

    public static int runBackupToolFromSameJvm( File neo4jHome, String... args )
    {
        try ( ExitCodeMemorizingOutsideWorld outsideWorld = new ExitCodeMemorizingOutsideWorld() )
        {
            AdminTool adminTool = new AdminTool( CommandLocator.fromServiceLocator(), outsideWorld, false );

            List<String> allArgs = new ArrayList<>();
            allArgs.add( "backup" );
            Collections.addAll( allArgs, args );

            Path homeDir = neo4jHome.getAbsoluteFile().toPath();
            Path configDir = homeDir.resolve( "conf" );
            adminTool.execute( homeDir, configDir, allArgs.toArray( new String[0] ) );

            return outsideWorld.getExitCode();
        }
    }
}

