# Improving Mortality Prediction in ICU using Spark Big Data Tool


## Instructions to run the code
### Dependencies :
```
Spark 1.3.0
Spark MLlib
Spark ML(High level API for machine learning pipelines)
Scala
sbt
Amazon Web Service
```

### Compile & Run :

I) Navigate to code folder

II) Download the necessary csv using the sql files in sql directory. Store these csv files to the data folder. Please take care of the naming convention.
```
PATIENT.csv
sofa.csv
sapsii.csv
saps.csv
apsiii.csv
oasis.csv
notes.csv
notesicurefine.csv
```

IV) Execute locally
```
sbt/sbt compile run
```


### Deploy on AWS. Cluster:
If you want to launch it on a AWS cluster:
```
~/spark/bin/spark-submit --class edu.gatech.cse8803.main.Main --master <Master node url>  --deploy-mode cluster  target/scala-2.10/cse8803_project-assembly-1.0.jar
