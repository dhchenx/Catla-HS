# Documents of Calta-HS

The use of Catla-HS must follow details of documents. Catla-HS requires redesign of MapReduce algorithms, flexible plain-text template and CMD-based uses. 

Note: CatlaUI is a desktop version of Catla to simplify the tuning process. See <a href='https://github.com/dhchenx/Catla-HS/tree/master/catla-ui'>here</a>

## Prerequisites
1. You should run Catla-HS in a Windows/Ubuntu computer located in <b>the same network</b> as Hadoop and Spark clusters. It means Catla-HS is able to access master host via network.
2. Standard <b>Java environment</b> on the computer should be properly installed. 
3. Hadoop must enable [Yarn Log Aggregation](https://mapr.com/docs/51/AdministratorGuide/YARNLogAggregation-Enabli_28214137-d3e129.html) by setting value of 'yarn.log-aggregation-enable' to true.  
4. Hadoop and Spark must enable their History Servers respectively.
5. Critical information of master host, like <b>username, userpassword, SSH port, etc.</b> must be known because Catla needs the information to run MapReduce jobs. 
6. You must <b>change the configuration of master host's information</b> in the env_* files in the example folder before you try to run any examples here. 
7. In your master host, please use 'sudo mkdir' command to create a new folder <b>/usr/hadoop_apps</b> for Hadoop jars and <b>usr/spark_apps</b> for Spark in Ubuntu and change the folder's permission to every-one access (sudo chmod -R 777). 
8. This project is built on Hadoop 2.7.2, which means it may work in all Hadoop 2.x.x versions. 