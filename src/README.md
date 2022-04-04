## To run the program

1. Package the code into jar file on local machine
2. Copy jar file into master node `scp -i <location of key> <location of jar file> <target location on hdfs>`
3. run `hadoop jar CZ4123_project-1.0-SNAPSHOT.jar <arguments>`

# Arguments
| Argument | Description          | Default | Examples                             |
|----------|----------------------|---------|--------------------------------------|
| -c       | Class name to run    | -       | init, max, min, cluster              |
| -i       | Input file location  | -       | /user/input, /user/intermediate/max  |
| -o       | Output file location | -       | /user/output, /user/intermediate/max |
