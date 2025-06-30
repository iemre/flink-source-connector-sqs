## SQS Source Connector for Apache Flink

AWS SQS source connector for Apache Flink (for 1.20) with at-least-once delivery.

If you're looking for an SQS Sink connector, there is already a community version available. Check out the Flink website.

### Important note

Wrote this as a stop-gap as I didn't want to go through the community review/voting/approval etc process. 

It's not been tested "in anger" or tuned properly, but works fine with moderate load. Contributions / forks welcome.

**Known issue with backpressured applications or failing checkpoints:** The SQS source connector asks AWS SQS to delete the messages it's received on successful checkpoints. If checkpoints are failing or delayed (e.g. due to backpressure), the application may process the same messages multiple times leading to increased latency. You could mitigate re-processing the same message multiple times by increasing the visibility timeout on the connector, however to solve the problem at the root, you'd need to address the delayed/failing checkpoints.


### Usage

`mvn install`

```
<dependency>
    <groupId>iemre.flink.connectors.sqs</groupId>
    <artifactId>flink-source-connector-aws-sqs</artifactId>
    <version>0.1.0</version>
</dependency>
```

Example code:

```
public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    
    // Take checkpoints every 60 seconds. 
    // The SQS connector tells AWS SQS to delete the read messages after a successful checkpoint. 
    env.enableCheckpointing(60000);

    SqsSource<String> sqsSource = new SqsSource.Builder<String>()
            .setQueueUrl("https://sqs.us-west-2.amazonaws.com/238499242/my-sqs")
            .setRegion("us-west-2")
            .setDeserializationSchema(new SimpleStringSchema())
            .setTypeInfo(BasicTypeInfo.STRING_TYPE_INFO)
            // make messages re-appear after 90 seconds, in case they are not deleted after a successful checkpoint
            .setVisibilityTimeoutSeconds(90)
            .setBatchSize(10)
            .setPollInterval(5000) // Poll every 5 seconds
            .build();

    DataStreamSource<String> sqsStream = env.fromSource(
            sqsSource,
            WatermarkStrategy.noWatermarks(),
            "SQS Source");

    sqsStream.setParallelism(1).print();
}

```
