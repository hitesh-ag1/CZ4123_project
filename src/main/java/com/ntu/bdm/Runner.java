package com.ntu.bdm;

import com.ntu.bdm.runner.MaxRunner;
import com.ntu.bdm.util.InputFileProcessor;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.ToolRunner;

import java.util.Objects;

public class Runner {
    private static String className = "";
    private static String inpath = "";
    private static String outpath = "";

    public static void main(String[] args) throws Exception {
        getCommandLineArguments(args);
        if (Objects.equals(className, "max")) {
            new MaxRunner(inpath, outpath);
        }
        else if (className.equals("init")){
            String[] arg = {inpath,outpath, "4"};
            ToolRunner.run(new InputFileProcessor(), arg);
        }
    }

    private static void getCommandLineArguments(String[] args) throws ParseException {
        // define options
        Options options = new Options();
        Option config;

        config = OptionBuilder
                .hasArg()
                .withLongOpt("class")
                .withDescription("choose which class to run")
                .create("c");
        options.addOption(config);

        config = OptionBuilder
                .hasArg()
                .withLongOpt("inputpath")
                .withDescription("The input path to read the data")
                .create("i");
        options.addOption(config);

        config = OptionBuilder
                .hasArg()
                .withLongOpt("outputpath")
                .withDescription("The output path to read the data")
                .create("o");
        options.addOption(config);

        // define parse
        CommandLine cmd;
        CommandLineParser parser = new BasicParser();
        HelpFormatter helper = new HelpFormatter();

        try {
            cmd = parser.parse(options, args);
            if (cmd.hasOption("c")) {
                String opt_config = cmd.getOptionValue("class");
                if (Objects.equals(opt_config.toUpperCase(), "MAX")) {
                    System.out.println("Getting maximum for the file");
                    className = "max";
                } else if (Objects.equals(opt_config.toUpperCase(), "INIT")) {
                    System.out.println("Generating key value pairs from raw data");
                    className = "init";
                } else System.out.println("Running as Unknown");
            }
            if (cmd.hasOption("i")) {
                String opt_config = cmd.getOptionValue("inputpath");
                System.out.println("Input path: " + opt_config);
                inpath = opt_config;
            } else System.out.println("Input path not found");

            if (cmd.hasOption("o")) {
                String opt_config = cmd.getOptionValue("outputpath");
                System.out.println("Output path: " + opt_config);
                outpath = opt_config;
            } else System.out.println("Output path not found");
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            helper.printHelp("Usage:", options);
            System.exit(0);
        }
    }

}
