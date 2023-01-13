# Delete all kinds in datastore with Dataflow

[ReadImpl.java](src%2Fmain%2Fjava%2Forg%2Fapache%2Fbeam%2Fsdk%2Fio%2Fgcp%2Fdatastore%2FReadImpl.java) was modified from original source code to take query as String input


```shell
mvn compile exec:java -Dexec.mainClass=vn.ycomm.gbye.dataflow.templates.FirestoreDeleteAll -Dexec.cleanupDaemonThreads=false "-Dexec.args=--runner=DataflowRunner --project=lezhincomix-alpha --stagingLocation=gs://alpha-temp/dataflow/staging --tempLocation=gs://alpha-temp/dataflow/temp --templateLocation=gs://alpha-temp/dataflow/template/FirestoreDeleteAll --region=us-central1"
```
