# terraform-starter

Public IP for Hadoop-Master
```
3.1.36.136
```

Hadoop-Master Environment Variables
```
$HADOOP_HOME = /usr/share/hadoop
$HADOOP_CONF_DIR = /usr/share/hadoop/etc/hadoop
```

To start Hadoop cluster
```
terraform apply -auto-approve -var "hadoop_worker_instance_count=<number of workers>"
```

To stop Hadoop cluster
```
terraform destroy -auto-approve
```

To SSH into Hadoop-Master
```
ssh -i "~/.ssh/aws_key" ec2-user@3.1.36.136
```

If error with SSH
```
ssh-keygen -R 3.1.36.136
```

Add data to Hadoop-Master
```
wget -O weather_data.txt https://drive.google.com/u/0/uc?id=1ik0QRq96hFwyaMMB0Q7yOmbg_PiefDar&export=download
```