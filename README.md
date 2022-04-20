# Getting Started

In our project, we used AWS to start Hadoop cluster for development purposes. However, the application we created can be
run on any Hadoop cluster. Please email us if you wish to test our application on AWS as we need to share AWS
credentials.

1. Start a Hadoop cluster.

2. Copy the `CZ4123.jar` from local to the Hadoop master.

3. Copy the `weatherData.csv` from local to the Hadoop master.

4. Place the `weatherData.csv` in HDFS with the following commands.

```
hdfs dfs -mkdir -p CZ4123/input
hdfs dfs -put weatherData.csv CZ4123/input
```

5. Run the `CZ4123.jar` file with the following command.

```
hadoop jar CZ4123.jar
```

6. Follow the instructions as shown in the UI. The input path of choice 1 should be `CZ4123/input/weatherData.csv`.
   The outpath path of choice 1 can be left as default. The input and output path of choice 2 - 6 can be left as
   default.

7. The output file after choice 1 - 6 is executed will be located in `CZ4123/kmean/part-r-00000`.