package com.ntu.bdm.ui;

public enum Manual {
    STOP("Stop the application"),
    INIT("Convert raw CSV to key value pairs"),
    MIN_MAX("Find the minimum and maximum"),
    MEAN_MED_SD("Find the mean, median and standard deviation"),
    POINT("Convert to Kmean features"),
    SELECTED("Filter and flatten features"),
    KMEAN("Run Kmean clustering");


    private final String description;

    Manual(String description) {
        this.description = description;
    }

    public static Manual get(int id) {
        try {
            return Manual.values()[id];
        } catch (Exception e) {
            return null;
        }
    }

    public static void printManual() {
        final String divider = new String(new char[64]).replace("\0", "=");
        Manual[] manuals = Manual.values();

        System.out.println();
        System.out.println(divider);
        System.out.println(
                String.format(
                        "Please choose a service by typing [%d-%d]:",
                        1, manuals.length - 1
                )
        );
        for (int i = 0; i < manuals.length; i++) {
            Manual manual = manuals[i];
            System.out.println(
                    String.format("%d: %s", i, manual.getDescription())
            );
        }
        System.out.println(divider);
    }

    public static void main(String[] args) {
        printManual();
    }

    public String getDescription() {
        return description;
    }

}

