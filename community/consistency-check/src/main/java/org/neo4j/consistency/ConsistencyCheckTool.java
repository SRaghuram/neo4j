/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.consistency;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheckNewUtils;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ConsistencyCheckTool
{
    private static final String RECOVERY = "recovery";
    private static final String CONFIG = "config";
    private static final String PROP_OWNER = "propowner";

    static interface ExitHandle
    {
        static final ExitHandle SYSTEM_EXIT = new ExitHandle()
        {
            @Override
            public void pull()
            {
                System.exit( 1 );
            }
        };

        void pull();
    }

    public static void main( String[] args ) throws IOException
    {
        args = checkToolType( args );
        switch ( FullCheckType.toolType )
        {
        case Legacy_Consistency_Check:
            org.neo4j.consistency.oldCC.ConsistencyCheckTool.main( args );
            break;
        case New_Fast_Consistency_Check:
            executeNewCC( args );
            break;
        case PropertyStore_Reorg:
            org.neo4j.propertystorereorg.PropertystoreReorgTool.main( args );
            //executeNewCC( args );
            break;
        }
    }

    private static String[] checkToolType( String[] args )
    {
        ArrayList<String> argsN = new ArrayList<String>();
        for ( int i = 0; i < args.length; i++ )
        {
            String arg = args[i];
            if ( args[i].startsWith( "-" ) || args[i].startsWith( "--" ) )
            {
                int offset = args[i].startsWith( "--" ) ? 2 : 1;
                arg = args[i].substring( offset );
            }
            if ( arg.equalsIgnoreCase( FullCheckType.LEGACY ) )
                FullCheckType.toolType = FullCheckType.ToolTypes.Legacy_Consistency_Check;
            else if ( arg.equalsIgnoreCase( FullCheckType.COMMAND ) )
            {
                FullCheckType.toolType = FullCheckType.ToolTypes.PropertyStore_Reorg;
                argsN.add( args[i++] );
                argsN.add( args[i] );
            }
            else
                argsN.add( args[i] );
        }
        return argsN.toArray( new String[0] );
    }

    public static void executeNewCC( String[] args ) throws IOException
    {
        // New CC 
        int index = 0;
        FullCheckNewUtils.Init();
        ArrayList<String> argsN = new ArrayList<String>();
        String graphDBString = null;
        boolean error = false;
        while ( index < args.length && !error )
        {
            if ( args[index].startsWith( "-" ) || args[index].startsWith( "--" ) )
            {
                int offset = args[index].startsWith( "--" ) ? 2 : 1;
                String arg = args[index].substring( offset );
                if ( arg.equalsIgnoreCase( "propowner" ) || arg.equalsIgnoreCase( "recovery" ) )
                    argsN.add( args[index++] );
                else if ( arg.equalsIgnoreCase( "config" ) )
                {
                    argsN.add( args[index++] );
                    argsN.add( args[index++] );
                }
                else if ( arg.toLowerCase().startsWith( "cclog" ) )
                {
                    FullCheckNewUtils.setLogFile( args[index++], args[index++] );
                }
                else if ( arg.toLowerCase().startsWith( "verbose" ) )
                {
                    FullCheckNewUtils.setVerbose( true );
                    index++;
                }
                else
                    error = true;
            }
            else if ( graphDBString == null )
                graphDBString = args[index++];
            else
            {
                error = true;
                System.out.println( "Error in parameters - " + args[index] + "\n" );
            }
        }
        if ( error || graphDBString == null )
        {
            ConsistencyCheckTool tool = new ConsistencyCheckTool( new ConsistencyCheckService(), System.err );
            System.out.println( usageNewCC( tool.getClass() ) );
            return;
        }
        String[] graphDBs = graphDBString.split( ";" );
        long[] totalTimeTaken = new long[graphDBs.length];
        for ( int i = 0; i < graphDBs.length; i++ )
        {
            try
            {
                argsN.add( graphDBs[i] );
                totalTimeTaken[i] = executeTool( argsN.toArray( new String[0] ) );
                argsN.remove( argsN.size() - 1 );
                FullCheckNewUtils.printProgressMsg( graphDBs, totalTimeTaken, i );
            }
            catch ( ToolFailureException e )
            {
                e.exitTool();
            }
            if ( FullCheckNewUtils.isVerbose() )
                System.out.println( "===========================================================" );
        }
    }

    private static long executeTool( String... args ) throws ToolFailureException, IOException
    {
        long timeTaken = 0;
        ;
        FullCheckNewUtils.startTime = System.currentTimeMillis();
        ConsistencyCheckTool tool = new ConsistencyCheckTool( new ConsistencyCheckService(), System.err );
        tool.run( args );
        timeTaken = (System.currentTimeMillis() - FullCheckNewUtils.startTime);
        System.out.println( "Time taken for " + FullCheckType.ToolTypes.New_Fast_Consistency_Check.name() + " to ["
                + "Full Consistency Check ]:[" + timeTaken + "] ms" );
        return timeTaken;
    }

    private final ConsistencyCheckService consistencyCheckService;
    private final StoreRecoverer recoveryChecker;
    private final GraphDatabaseFactory dbFactory;
    private final PrintStream systemError;
    private final ExitHandle exitHandle;

    ConsistencyCheckTool( ConsistencyCheckService consistencyCheckService, PrintStream systemError )
    {
        this( consistencyCheckService, new StoreRecoverer(), new GraphDatabaseFactory(), systemError,
                ExitHandle.SYSTEM_EXIT );
    }

    ConsistencyCheckTool( ConsistencyCheckService consistencyCheckService, StoreRecoverer recoveryChecker,
            GraphDatabaseFactory dbFactory, PrintStream systemError, ExitHandle exitHandle )
    {
        this.consistencyCheckService = consistencyCheckService;
        this.recoveryChecker = recoveryChecker;
        this.dbFactory = dbFactory;
        this.systemError = systemError;
        this.exitHandle = exitHandle;
    }

    void run( String... args ) throws ToolFailureException, IOException
    {
        Args arguments = Args.withFlags( RECOVERY, PROP_OWNER ).parse( args );
        String storeDir = determineStoreDirectory( arguments );
        System.out.println( "------------>[" + FullCheckType.ToolTypes.New_Fast_Consistency_Check.name() + "] "
                + storeDir + "<---------------" );
        Config tuningConfiguration = readTuningConfiguration( storeDir, arguments );
        attemptRecoveryOrCheckStateOfLogicalLogs( arguments, storeDir );
        StringLogger logger = StringLogger.SYSTEM;
        try
        {
            consistencyCheckService.runFullConsistencyCheck( storeDir, tuningConfiguration,
                    ProgressMonitorFactory.textual( System.err ), logger );
        }
        catch ( ConsistencyCheckIncompleteException e )
        {
            throw new ToolFailureException( "Check aborted due to exception", e );
        }
        finally
        {
            logger.flush();
        }
        FullCheckNewUtils.NewCCCache.close();
    }

    private void attemptRecoveryOrCheckStateOfLogicalLogs( Args arguments, String storeDir )
    {
        if ( arguments.getBoolean( RECOVERY, false, true ) )
        {
            dbFactory.newEmbeddedDatabase( storeDir ).shutdown();
        }
        else
        {
            try
            {
                if ( recoveryChecker.recoveryNeededAt( new File( storeDir ) ) )
                {
                    systemError.print( lines(
                            "Active logical log detected, this might be a source of inconsistencies.",
                            "Consider allowing the database to recover before running the consistency check.",
                            "Consistency checking will continue, abort if you wish to perform recovery first.",
                            "To perform recovery before checking consistency, use the '--recovery' flag." ) );
                    exitHandle.pull();
                }
            }
            catch ( IOException e )
            {
                systemError.printf( "Failure when checking for recovery state: '%s', continuing as normal.%n", e );
            }
        }
    }

    private String determineStoreDirectory( Args arguments ) throws ToolFailureException
    {
        List<String> unprefixedArguments = arguments.orphans();
        String storeDir = unprefixedArguments.get( 0 );
        if ( !new File( storeDir ).isDirectory() )
        {
            throw new ToolFailureException( lines( String.format( "'%s' is not a directory", storeDir ) ) + usage() );
        }
        return storeDir;
    }

    private Config readTuningConfiguration( String storeDir, Args arguments ) throws ToolFailureException
    {
        Map<String,String> specifiedProperties = stringMap();
        String propertyFilePath = arguments.get( CONFIG, null );
        if ( propertyFilePath != null )
        {
            File propertyFile = new File( propertyFilePath );
            try
            {
                specifiedProperties = MapUtil.load( propertyFile );
            }
            catch ( IOException e )
            {
                throw new ToolFailureException( String.format( "Could not read configuration properties file [%s]",
                        propertyFilePath ), e );
            }
        }
        specifiedProperties.put( GraphDatabaseSettings.store_dir.name(), storeDir );
        return new Config( specifiedProperties, GraphDatabaseSettings.class, ConsistencyCheckSettings.class );
    }

    private static final String usageMsg =
            " [-propowner] [-recovery] [-cclog[:console] <logfile>] [-config <neo4j.properties>] [-command getstats| fixpropertystore] [-verbose] [-legacy] <storedir>[;<storedir>]"
                    + "\nWHERE:   <storedir>        is the path to the store(s) to check, separated by ;"
                    + "\n         -recovery          to perform recovery on the store before checking"
                    + "\n         -property_reorg    to rearrange property/string/array store for optimal performance before checking"
                    + "\n         <neo4j.properties> is the location of an optional properties file"
                    + "\n                            containing tuning parameters for the consistency check"
                    + "\n         -cclog:console <logfile> log file to store messages for new consistency check."
                    + "\n				Optional 'console' is to print on console in addition to logs"
                    + "\n         -legacy        when present it runs exactly as older version of consistency check"
                    + "\n         -command getstats  to analyse and display the scatter characteristics of property store"
                    + "\n         -command fixpropertystore - this is to reorganize property/array/string store optimally"
                    + "\n         -verbose           to print the detailed error messages";

    private String usage()
    {
        return usageNewCC( getClass() );
    }

    private static String usageNewCC( Class className )
    {
        StringBuilder usage =
                new StringBuilder(
                        "USAGE: java -cp \"neo4j-consistencycheck-new-2.2.3.jar;<2.2.x install Dir>/lib/*.jar\" " );
        usage.append( ' ' ).append( className.getCanonicalName() );
        usage.append( lines( usageMsg ) );
        return usage.toString();
    }

    private static String lines( String... content )
    {
        StringBuilder result = new StringBuilder();
        for ( String line : content )
        {
            result.append( line ).append( System.getProperty( "line.separator" ) );
        }
        return result.toString();
    }

    class ToolFailureException extends Exception
    {
        ToolFailureException( String message )
        {
            super( message );
        }

        ToolFailureException( String message, Throwable cause )
        {
            super( message, cause );
        }

        void exitTool()
        {
            System.err.println( getMessage() );
            if ( getCause() != null )
            {
                getCause().printStackTrace( System.err );
            }
            exitHandle.pull();
        }
    }
}
