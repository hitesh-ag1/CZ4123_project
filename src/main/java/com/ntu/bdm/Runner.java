package com.ntu.bdm;

import com.ntu.bdm.runner.*;
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
        switch (className) {
            case "max":
                new MaxRunner(inpath, outpath);
                break;
            case "mean":
                new MeanRunner(inpath, outpath);
                break;
            case "point":
                new PointRunner(inpath, outpath);
                break;
            case "select":
                new SelectedFieldRunner(inpath, outpath);
                break;
            case "kmean":
                new KmeanRunner(inpath, outpath);
                break;

            case "init":
                String[] arg = {inpath, outpath, "4"};
                ToolRunner.run(new InputFileProcessor(), arg);
                break;
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
                .isRequired()
                .withLongOpt("inputpath")
                .withDescription("The input path to read the data")
                .create("i");
        options.addOption(config);

        config = OptionBuilder
                .hasArg()
                .isRequired()
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
                switch (opt_config.toUpperCase()) {
                    case "MAX":
                        System.out.println("Getting maximum for the file");
                        className = "max";
                        break;
                    case "MEAN":
                        System.out.println("Getting mean, median and SD for the file");
                        className = "mean";
                        break;
                    case "INIT":
                        System.out.println("Generating key value pairs from raw data");
                        className = "init";
                        break;
                    case "POINT":
                        System.out.println("Converting to points of feature");
                        className = "point";
                        break;
                    case "SELECT":
                        System.out.println("Filter and flatten the user input field");
                        className = "select";
                        break;
                    case "KMEAN":
                        System.out.println("Running kmean clustering");
                        className = "kmean";
                        break;
                    default:
                        System.out.println("Running as Unknown");
                        break;
                }
            }
            if (cmd.hasOption("i")) {
                String opt_config = cmd.getOptionValue("inputpath");
                System.out.println("Input path: " + opt_config);
                inpath = opt_config;
            } else throw new Error("Input path arguments not found");

            if (cmd.hasOption("o")) {
                String opt_config = cmd.getOptionValue("outputpath");
                System.out.println("Output path: " + opt_config);
                outpath = opt_config;
            } else throw new Error("Input path arguments not found");
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            helper.printHelp("Usage:", options);
            System.exit(0);
        }
    }

}
