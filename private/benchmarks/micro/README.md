Available Commands
--------------
        $ java -jar target/micro-benchmarks.jar help
        usage: bench <command> [<args>]
        
        The most commonly used bench commands are:
            config       Creates benchmark configuration file
            help         Display help information
            ls           prints available groups and their benchmarks
            run          runs benchmarks
            run-export   runs benchmarks and exports results as JSON


Running Locally
--------------

        $ java -jar target/micro-benchmarks.jar help run
        NAME
                bench run - runs benchmarks
        
        SYNOPSIS
                bench run [--config <Benchmark Configuration>] [--jmh <JMH Args>]
                        [--jvm_args <JVM Args>] [--neo4j_config <Neo4j Configuration>]
                        [--neo4j_package_for_jvm_args <Neo4j package containing JVM args>]
                        [--profile-async] [--profile-jfr]
                        [--profiles-dir <Profile recordings output directory>]
                        [--threads <Thread count>]
        
        OPTIONS
                --config <Benchmark Configuration>
                    Benchmark configuration: enable/disable tests, specify parameters to
                    run with
        
                --jmh <JMH Args>
                    Standard JMH CLI args. These will be applied on top of other
                    provided configuration
        
                --jvm_args <JVM Args>
                    JVM arguments that benchmark was run with (e.g., '-XX:+UseG1GC
                    -Xms4g -Xmx4g')
        
                --neo4j_config <Neo4j Configuration>
                    Neo4j configuration used during benchmark
        
                --neo4j_package_for_jvm_args <Neo4j package containing JVM args>
                    Extract default product JVM args from Neo4j tar.gz package
        
                --profile-async
                    Run with Async profiler
        
                --profile-jfr
                    Run with JFR profiler
        
                --profiles-dir <Profile recordings output directory>
                    Where to collect profiler recordings for executed benchmarks
        
                --threads <Thread count>
                    Number of threads to run benchmarks with


Custom Benchmark Execution
--------------
A specific set of benchmarks are run by default. 
For more control, it is possible to override the default mode by providing a benchmark configuration file.

        $ java -jar target/micro-benchmarks.jar help config
        NAME
                bench config - Creates benchmark configuration file
        
        SYNOPSIS
                bench config
                bench config benchmarks [(-w | --with-disabled)]
                        --path <Configuration Path> [(-v | --verbose)]
                bench config default [(-w | --with-disabled)]
                        --path <Configuration Path> [(-v | --verbose)]
                bench config groups [(-w | --with-disabled)] --path <Configuration Path>
                        [(-v | --verbose)]
        
        COMMANDS
                With no arguments, creates a benchmark configuration file limited to
                specified groups
        
                default
                    creates a benchmark configuration file limited to specified groups
        
                    With --with-disabled option, Write disabled benchmarks, in addition
                    to the enabled
        
                    With --path option, Path to export the generated benchmark
                    configuration file to
        
                    With --verbose option, Write benchmark parameters to config file, in
                    addition to enable/disable info
        
                groups
                    creates a benchmark configuration file limited to specified groups
        
                    With --with-disabled option, Write disabled benchmarks, in addition
                    to the enabled
        
                    With --path option, Path to export the generated benchmark
                    configuration file to
        
                    With --verbose option, Write benchmark parameters to config file, in
                    addition to enable/disable info
        
                benchmarks
                    creates a benchmark configuration file limited to specified
                    benchmarks
        
                    With --with-disabled option, Write disabled benchmarks, in addition
                    to the enabled
        
                    With --path option, Path to export the generated benchmark
                    configuration file to
        
                    With --verbose option, Write benchmark parameters to config file, in
                    addition to enable/disable info


Once you have created a configuration file you can modify it:

        java -jar target/micro-benchmarks.jar config default  \
            --path path/to/exported/default.conf

You now have a configuration file (`path/to/exported/default.conf`), populated with default settings. 
Take a look inside and customize.

You can also enable just the benchmarks from specific groups (e.g., Core API, Cypher, Page 
Cache):

        java -jar target/micro-benchmarks.jar config groups  \
            --path path/to/exported/default.conf  \
            'Core API' \
            Cypher

Likewise, you can enable just specific benchmarks:

        java -jar target/micro-benchmarks.jar config benchmarks  \
            --path path/to/exported/default.conf  \
            com.neo4j.bench.micro.benchmarks.core.ReadById \
            com.neo4j.bench.micro.benchmarks.core.ReadAll

Working on the Benchmark GUI - 'Alacrity'
--------------

The user interface for the benchmarks exists in a separate repository independent of Neo4j version:

* Git repo and README: https://github.com/neo-technology/alacrity
* Live website: http://alacrity.neohq.net

If you would like to enhance the Alacrity GUI refer to the instructions on the [README](https://github.com/neo-technology/alacrity/blob/master/README.md) for checkout and deploying Alacrity.
