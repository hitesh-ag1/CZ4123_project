package com.ntu.bdm;

import com.ntu.bdm.runner.*;
import com.ntu.bdm.util.InputFileProcessor;
import org.apache.commons.cli.*;
import org.apache.hadoop.util.ToolRunner;

public class Runner {
    private static String className = "";
    private static String inpath = "";
    private static String outpath = "";
    private static String inpath2 = "";
    private static int numIte = 3;

    public static void main(String[] args) throws Exception {
        getCommandLineArguments(args);
        switch (className) {
            case "global_min_max":
                new GlobalMinMaxRunner(inpath, outpath);
                break;
            case "mean":
                new StatsRunner(inpath, outpath);
                break;
            case "point":
                new PointRunner(inpath, outpath, "/tmp/global");
                break;
            case "select":
                new SelectedFieldRunner(inpath, outpath, null);
                break;
            case "kmean":
                new KmeanRunner(inpath, outpath, 3, 3, true);
                break;
            case "output":
                new OutputRunner(inpath, outpath, 3);
                break;
            case "norm":
                new NormRunner(inpath, outpath, inpath2);
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

        config = OptionBuilder
                .hasArg()
                .withLongOpt("numIteration")
                .withDescription("The output path to read the data")
                .create("ite");
        options.addOption(config);

        config = OptionBuilder
                .hasArg()
                .withLongOpt("numIteration")
                .withDescription("The output path to read the data")
                .create("ite");
        options.addOption(config);

        config = OptionBuilder
                .hasArg()
                .withLongOpt("inputpath2")
                .withDescription("The input path to read the data")
                .create("i2");
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
                        className = "global_min_max";
                        break;
                    case "MEAN":
                        System.out.println("Getting mean, median and SD for the file");
                        className = "mean";
                        break;

                    case "NORM":
                        System.out.println("Normalizing data file");
                        className = "norm";
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
                    case "OUTPUT":
                        System.out.println("Organising into final output format");
                        className = "output";
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
            } else throw new Error("Output path arguments not found");

            if (cmd.hasOption("ite")) {
                String opt_config = cmd.getOptionValue("numIteration");
                System.out.println("Number of Kmean iteration: " + opt_config);
                numIte = Integer.parseInt(opt_config);
            } else throw new Error("Input path arguments not found");

            if (cmd.hasOption("i2")) {
                String opt_config = cmd.getOptionValue("inputpath2");
                System.out.println("Input path 2: " + opt_config);
                inpath2 = opt_config;
            }

        } catch (ParseException e) {
            System.out.println(e.getMessage());
            helper.printHelp("Usage:", options);
            System.exit(0);
        }
    }

}
