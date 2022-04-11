package com.ntu.bdm.ui;

import com.ntu.bdm.runner.*;
import com.ntu.bdm.util.InputFileProcessor;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.Arrays;

public class TasksUserInterface {
    public TasksUserInterface() {
    }

    public static void main(String[] args) throws Exception {
        TasksUserInterface ui = new TasksUserInterface();
        ui.runInitUI();
    }

    private void printHeader(String message) {
        System.out.println();
        System.out.println(new String(new char[message.length()]).replace("\0", "="));
        System.out.println(message);
        System.out.println(new String(new char[message.length()]).replace("\0", "="));
    }

    private void printTask(String task, String inPath, String outPath) {
        System.out.println();
        System.out.println("Running the task: " + task);
        System.out.println("Input path: " + inPath);
        System.out.println("Output path: " + outPath);
        System.out.println();
        System.out.println("Map reduce log");
        System.out.println(new String(new char[30]).replace("\0", "-"));
    }

    private String askPath(String description) {
        System.out.println("Press ENTER if you want to use default path CZ4123");
        return SafeScanner.readLine(String.format("Enter %s Path = ", description));
    }

    private int askIte() {
        System.out.println("Put -1 if you want to use default value = 3");
        return SafeScanner.readInt("Enter number of iteration = ");
    }

    private int askRandomCentroid() {
        System.out.println("Put 1 if you want to use random centroid, 0 to use first K points as centroids");
        return SafeScanner.readInt("Random generate centroid = ");
    }

    private int askNumCluster() {
        System.out.println("Put -1 if you want to use default value = 3");
        return SafeScanner.readInt("Enter number of cluster = ");
    }

    private String askField() {
        printHeader("Choose the field");
        Field[] fields = Field.values();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            System.out.println(
                    String.format("%d: %s", i, field.getDescription())
            );
        }
        int choice = SafeScanner.readInt("Enter the number = ");
        return fields[choice].name();
    }

    private String askStats() {
        printHeader("Choose the calculated statistical value");
        Stat[] stats = Stat.values();
        for (int i = 0; i < stats.length; i++) {
            Stat stat = stats[i];
            System.out.println(
                    String.format("%d: %s", i, stat.getDescription())
            );
        }
        int choice = SafeScanner.readInt("Enter the number = ");
        return stats[choice].name();
    }

    private ArrayList<String> askCriterion(){
        printHeader("Choose the field and stats");
        ArrayList<String> res = new ArrayList<>();
        int choice = 1;
        while (choice == 1){
            res.add(askField() + "_" + askStats());
            choice = SafeScanner.readInt("Put 1 to add more, 0 to stop = ");
        }
        return res;
    }

    void runInitUI() throws Exception {
        String header = "Converting raw CSV to key value pairs";
        this.printHeader(header);
        String inPath = this.askPath("Input");
        String outPath = this.askPath("Output");

        if (inPath.isEmpty()) {
            inPath = "CZ4123/input/exampleWithIndex.csv";
        }
        if (outPath.isEmpty()) {
            outPath = "CZ4123/intermediate";
        }
        printTask(header, inPath, outPath);
        String[] arg = {inPath, outPath, "4"};
        ToolRunner.run(new InputFileProcessor(), arg);
    }

    void runNormaliseUI() throws Exception {
        String header = "Normalising data and indexing date";
        this.printHeader(header);
        String inPath = this.askPath("Input");
        String outPath = this.askPath("Output");

        if (inPath.isEmpty()) {
            inPath = "CZ4123/intermediate";
        }
        if (outPath.isEmpty()) {
            outPath = "CZ4123/normalised";
        }
        printTask(header, inPath, outPath);
        String[] arg = {inPath, outPath, "4"};
        ToolRunner.run(new InputFileProcessor(), arg);
    }


    void runStatsUI() throws Exception {
        String header = "Finding the statistical values";
        this.printHeader(header);
        String inPath = this.askPath("Input");
        String outPath = this.askPath("Output");

        if (inPath.isEmpty()) {
            inPath = "CZ4123/normalise";
        }
        if (outPath.isEmpty()) {
            outPath = "CZ4123/stats/";
        }
        printTask(header, inPath, outPath);
        new StatsRunner(inPath, outPath);
    }

    void runPointUI() throws Exception {
        String header = "Converting the statistic to Kmean features";
        this.printHeader(header);
        String inPath = this.askPath("Input");
        String outPath = this.askPath("Output");

        if (inPath.isEmpty()) {
            inPath = "CZ4123/stats/*/*";
        }
        if (outPath.isEmpty()) {
            outPath = "CZ4123/points";
        }
        printTask(header, inPath, outPath);
        new PointRunner(inPath, outPath);
    }

    void runSelectedFieldUI() throws Exception {
        String header = "Filter and flatten the selected criterion ";
        this.printHeader(header);
        String inPath = this.askPath("Input");
        String outPath = this.askPath("Output");
        ArrayList<String> criterion = askCriterion();

        if (inPath.isEmpty()) {
            inPath = "CZ4123/points";
        }
        if (outPath.isEmpty()) {
            outPath = "CZ4123/selected";
        }

        if (criterion.size() == 0){
            criterion.add("TMP_MAX");
            criterion.add("HUM_MAX");
        }

        printTask(header + ": " + criterion.toString(), inPath, outPath);
        new SelectedFieldRunner(inPath, outPath, criterion);
    }

    void runKmeanUI() throws Exception {
        String header = "Running Kmeans";
        this.printHeader(header);
        String inPath = this.askPath("Input");
        String outPath = this.askPath("Output");
        int numIte = this.askIte();
        int numCluster = this.askNumCluster();
        int randomCentroidInit = this.askRandomCentroid();

        if (inPath.isEmpty()) {
            inPath = "CZ4123/selected";
        }
        if (outPath.isEmpty()) {
            outPath = "CZ4123/kmean";
        }
        if (numIte == -1) {
            numIte = 3;
        }
        if (numCluster == -1) {
            numCluster = 3;
        }

        printTask(header, inPath, outPath);
        new KmeanRunner(inPath, outPath, numCluster, numIte, randomCentroidInit > 0);

        header = "Linking station to the calculated centroids\n (Check /tmp/centroid.txt for final centroid values)";
        this.printHeader(header);
        new OutputRunner(inPath, outPath, numCluster);
    }

    enum Field {
        TMP("Temperature"),
        HUM("Humidity");

        private final String description;

        Field(String description) {
            this.description = description;
        }

        public static Field get(int id) {
            try {
                return Field.values()[id];
            } catch (Exception e) {
                return null;
            }
        }

        public String getDescription() {
            return description;
        }
    }

    enum Stat {
        MAX("Maximum"),
        MIN("Minimum"),
        MEAN("Mean"),
        MEDIAN("Median"),
        SD("Standard Deviation");

        private final String description;

        Stat(String description) {
            this.description = description;
        }

        public static Stat get(int id) {
            try {
                return Stat.values()[id];
            } catch (Exception e) {
                return null;
            }
        }

        public String getDescription() {
            return description;
        }
    }
}
