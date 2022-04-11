package com.ntu.bdm.ui;
public class MainRunner {
    private static Manual askUserChoice() throws Exception {
        int id = SafeScanner.readInt("Your choice = ");
        Manual choice = Manual.get(id);
        if (choice == null) {
            throw new Exception(String.format("Invalid user choice %d", id));
        }
        return choice;
    }

    public MainRunner() {
        boolean shouldStop = false;
        TasksUserInterface ui = new TasksUserInterface();
        while (!shouldStop) {
            Manual.printManual();
            try {
                Manual userChoice = askUserChoice();
                switch (userChoice) {
                    case INIT:
                        ui.runInitUI();
                        break;
                    case NORMALISE:
                        ui.runNormaliseUI();
                        break;
                    case STATS:
                        ui.runStatsUI();
                        break;
                    case POINT:
                        ui.runPointUI();
                        break;
                    case SELECTED:
                        ui.runSelectedFieldUI();
                        break;
                    case KMEAN:
                        ui.runKmeanUI();
                        break;
                    case STOP:
                        shouldStop = true;
                        break;
                    default:
                        break;
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        SafeScanner.closeReader();
        System.out.println("Terminating the application...");
    }

    public static void main(String[] args) {
        new MainRunner();
    }
}
