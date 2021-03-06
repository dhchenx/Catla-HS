## Examples of Catla-HS
Each folder provides an example of use of template rules to organize necessary information to run on Hadoop. 

## Note
The information of Hadoop cluster in env_* file(s) should be modified according the user's actual Hadoop environment to make it work. 

## Description of each folder
1) /task_wordcount: submit a MapReduce job to a Hadoop cluster.
2) /project_wordcount: submit a series of MapReduce job operations to a Hadoop cluster.
3) /tuning_wordcount: tuning MapReduce job performance according to the configuration of parameters in /tuning subfolder. 

## Instructions to run these examples
1) Open your Windows Command program; CD to the root folder of '/examples' folder
2) Copy the Catla.jar from '/catla-dist' folder to '/examples' folder
3) Using the shell code:
```bash
java -jar Catla-HS.jar -tool ...
```
The above command is used to run these examples. (Details of the input arguments please see <a href='https://github.com/dhchenx/Catla/blob/master/docs/catla-usage.md'>Usage of Catla</a>)

