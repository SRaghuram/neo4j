package com.neo4j.bench.macro.workload;

import com.google.common.collect.Sets;
import com.neo4j.bench.client.results.ForkDirectory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.neo4j.bench.client.util.BenchmarkUtil.assertFileExists;

import static java.util.stream.Collectors.toSet;

public abstract class Parameters
{
    private static final String PARAMETERS_FILE = "file";
    private static final String IS_LOOPABLE = "isLoopable";
    static final boolean IS_LOOPABLE_DEFAULT = true;
    private static final String CSV_FILE = "csv";
    private static final String CSV_PARAMETER_KEY = "csv_filename";
    private static final Set<String> ALL_VALID_KEYS = Sets.newHashSet( PARAMETERS_FILE, CSV_FILE, IS_LOOPABLE );

    public abstract boolean isLoopable();

    public abstract ParametersReader create( ForkDirectory forkDirectory ) throws Exception;

    static Parameters empty()
    {
        return new EmptyParameters();
    }

    static Parameters from( Map<String,Object> parametersEntry, Path workloadDir )
    {
        assertConfigHasValidKeys( parametersEntry );
        if ( parametersEntry.containsKey( PARAMETERS_FILE ) )
        {
            Path parametersFile = workloadDir.resolve( (String) parametersEntry.get( PARAMETERS_FILE ) );
            boolean isLoopable = (boolean) parametersEntry.getOrDefault( IS_LOOPABLE, IS_LOOPABLE_DEFAULT );
            return new FileParameters( parametersFile, isLoopable );
        }
        else if ( parametersEntry.containsKey( CSV_FILE ) )
        {
            Path csvFile = workloadDir.resolve( (String) parametersEntry.get( CSV_FILE ) );
            assertFileExists( csvFile );
            return new CsvParameters( csvFile );
        }
        else
        {
            throw new WorkloadConfigException( WorkloadConfigError.NO_PARAM_FILE );
        }
    }

    private static void assertConfigHasValidKeys( Map<String,Object> parametersConfig )
    {
        Set<String> invalidKeys = parametersConfig.keySet().stream()
                                                  .filter( key -> !ALL_VALID_KEYS.contains( key ) )
                                                  .collect( toSet() );
        if ( !invalidKeys.isEmpty() )
        {
            throw new RuntimeException( "Parameters config contained unrecognized keys: " + invalidKeys );
        }
        if ( parametersConfig.containsKey( PARAMETERS_FILE ) && parametersConfig.containsKey( CSV_FILE ) )
        {
            throw new RuntimeException( "Parameters config must not contain both " + PARAMETERS_FILE + " and " + CSV_FILE );
        }
    }

    private static class FileParameters extends Parameters
    {
        private final Path parametersFile;
        private final boolean isLoopable;

        private FileParameters( Path parametersFile, boolean isLoopable )
        {
            this.parametersFile = parametersFile;
            this.isLoopable = isLoopable;
        }

        @Override
        public boolean isLoopable()
        {
            return isLoopable;
        }

        @Override
        public ParametersReader create( ForkDirectory forkDirectory ) throws Exception
        {
            if ( isLoopable )
            {
                try ( FileParametersReader fileParametersReader = new FileParametersReader( parametersFile ) )
                {
                    return LoopingParametersReader.from( fileParametersReader );
                }
            }
            else
            {
                return new FileParametersReader( parametersFile );
            }
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + " ( " + parametersFile.toAbsolutePath() + " )";
        }
    }

    private static class CsvParameters extends Parameters
    {
        private final Path resourceCsv;

        private CsvParameters( Path resourceCsv )
        {
            this.resourceCsv = resourceCsv;
        }

        @Override
        public boolean isLoopable()
        {
            return true;
        }

        @Override
        public ParametersReader create( ForkDirectory forkDirectory ) throws IOException
        {
            Path forkCsv = forkDirectory.findOrCreate( resourceCsv.getFileName().toString() );
            Files.copy( resourceCsv, forkCsv, StandardCopyOption.REPLACE_EXISTING );
            Map<String,Object> parameters = new HashMap<>();
            parameters.put( CSV_PARAMETER_KEY, "file:///" + forkCsv.toAbsolutePath() );

            // TODO remove, just for debug
            System.out.println( "Path to LOAD CSV resource" );
            System.out.println( "Resource :              " + resourceCsv.toAbsolutePath() );
            System.out.println( "Actual Parameter Value: " + parameters.get( CSV_PARAMETER_KEY ) );

            return new ParametersReader()
            {
                @Override
                public boolean hasNext()
                {
                    return true;
                }

                @Override
                public Map<String,Object> next()
                {
                    return parameters;
                }

                @Override
                public void close()
                {

                }
            };
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }

    private static class EmptyParameters extends Parameters
    {
        @Override
        public boolean isLoopable()
        {
            return true;
        }

        @Override
        public ParametersReader create( ForkDirectory forkDirectory )
        {
            return new ParametersReader()
            {
                @Override
                public boolean hasNext()
                {
                    return true;
                }

                @Override
                public Map<String,Object> next()
                {
                    return Collections.emptyMap();
                }

                @Override
                public void close()
                {

                }
            };
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName();
        }
    }
}
