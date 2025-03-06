/**
 * Copyright 2025 Emre Kartoglu
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package iemre.flink.connectors.sqs;

import com.google.common.collect.Lists;
import lombok.Builder;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

/**
 * A FLIP-27 source implementation for Amazon SQS.
 * Support for at-least-once only: Read SQS messages are deleted after successful checkpoints. There is a possibility
 * of re-reading / re-processing a message if a checkpoint fails or the job is restarted before a checkpoint is taken.
 */
@Builder
public class SqsSource<OUT> implements Source<OUT, SqsSourceSplit, SqsSourceSplitState>, ResultTypeQueryable<OUT> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SqsSource.class);

    private final String queueUrl;
    private final String regionName;
    private final DeserializationSchema<OUT> deserializationSchema;
    private final TypeInformation<OUT> typeInfo;
    private final int batchSize;
    private final int pollInterval;
    private final int visibilityTimeoutSeconds;

    public SqsSource(
            String queueUrl,
            String regionName,
            DeserializationSchema<OUT> deserializationSchema,
            TypeInformation<OUT> typeInfo,
            int batchSize,
            int visibilityTimeoutSeconds,
            int pollInterval) {
        this.queueUrl = queueUrl;
        this.regionName = regionName;
        this.deserializationSchema = deserializationSchema;
        this.typeInfo = typeInfo;
        this.batchSize = batchSize;
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
        this.pollInterval = pollInterval;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, SqsSourceSplit> createReader(SourceReaderContext readerContext) {
        return new SqsSourceReader<>(
                regionName,
                deserializationSchema,
                batchSize,
                visibilityTimeoutSeconds,
                pollInterval,
                readerContext);
    }

    @Override
    public SplitEnumerator<SqsSourceSplit, SqsSourceSplitState> createEnumerator(
            SplitEnumeratorContext<SqsSourceSplit> enumContext) {
        return new SqsSourceSplitEnumerator(queueUrl, regionName, enumContext);
    }

    @Override
    public SplitEnumerator<SqsSourceSplit, SqsSourceSplitState> restoreEnumerator(
            SplitEnumeratorContext<SqsSourceSplit> enumContext,
            SqsSourceSplitState checkpoint) {
        return new SqsSourceSplitEnumerator(queueUrl, regionName, enumContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<SqsSourceSplit> getSplitSerializer() {
        return new SqsSourceSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<SqsSourceSplitState> getEnumeratorCheckpointSerializer() {
        return new SqsSourceSplitStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return typeInfo;
    }

}

class SqsSourceSplit implements SourceSplit, Serializable {
    private final String splitId;
    private final String queueUrl;

    public SqsSourceSplit(String splitId, String queueUrl) {
        this.splitId = splitId;
        this.queueUrl = queueUrl;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public String getQueueUrl() {
        return queueUrl;
    }
}

class SqsSourceSplitState implements Serializable {
    private final List<SqsSourceSplit> assignedSplits;

    public SqsSourceSplitState() {
        this.assignedSplits = new ArrayList<>();
    }

    public SqsSourceSplitState(List<SqsSourceSplit> assignedSplits) {
        this.assignedSplits = assignedSplits;
    }

    public List<SqsSourceSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public void addAssignedSplit(SqsSourceSplit split) {
        assignedSplits.add(split);
    }
}

class SqsSourceSplitSerializer implements SimpleVersionedSerializer<SqsSourceSplit> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(SqsSourceSplit split) {
        // Implement serialization logic
        // For simplicity, we assume a basic serialization approach
        String serialized = split.splitId() + ";" + split.getQueueUrl();
        return serialized.getBytes();
    }

    @Override
    public SqsSourceSplit deserialize(int version, byte[] serialized) {
        // Implement deserialization logic
        String data = new String(serialized);
        String[] parts = data.split(";");
        return new SqsSourceSplit(parts[0], parts[1]);
    }
}

class SqsSourceSplitStateSerializer implements SimpleVersionedSerializer<SqsSourceSplitState> {
    private final SqsSourceSplitSerializer splitSerializer = new SqsSourceSplitSerializer();

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(SqsSourceSplitState state) {
        // Implement serialization logic
        // Simplified for clarity
        StringBuilder sb = new StringBuilder();
        for (SqsSourceSplit split : state.getAssignedSplits()) {
            sb.append(new String(splitSerializer.serialize(split))).append(",");
        }
        return sb.toString().getBytes();
    }

    @Override
    public SqsSourceSplitState deserialize(int version, byte[] serialized) {
        String data = new String(serialized);
        if (data.isEmpty()) {
            return new SqsSourceSplitState();
        }

        String[] splitData = data.split(",");
        List<SqsSourceSplit> splits = new ArrayList<>();
        for (String splitStr : splitData) {
            if (!splitStr.isEmpty()) {
                splits.add(splitSerializer.deserialize(version, splitStr.getBytes()));
            }
        }
        return new SqsSourceSplitState(splits);
    }
}

class SqsSourceSplitEnumerator implements SplitEnumerator<SqsSourceSplit, SqsSourceSplitState> {
    private final SplitEnumeratorContext<SqsSourceSplit> context;
    private final String queueUrl;
    private final String regionName;
    private final List<SqsSourceSplit> assignedSplits;
    private transient SqsClient sqsClient;

    public SqsSourceSplitEnumerator(
            String queueUrl,
            String regionName,
            SplitEnumeratorContext<SqsSourceSplit> context) {
        this(queueUrl, regionName, context, new SqsSourceSplitState());
    }

    public SqsSourceSplitEnumerator(
            String queueUrl,
            String regionName,
            SplitEnumeratorContext<SqsSourceSplit> context,
            SqsSourceSplitState state) {
        this.queueUrl = queueUrl;
        this.regionName = regionName;
        this.context = context;
        this.assignedSplits = new ArrayList<>(state.getAssignedSplits());
    }

    @Override
    public void start() {
        sqsClient = SqsClient.builder()
                .region(Region.of(regionName))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        // SQS splits are all same, there is no way to specify offset or chunks or anything, there is just 1 queue URL
        SqsSourceSplit split = new SqsSourceSplit("sqs-split-" + queueUrl.hashCode(), queueUrl);
        assignedSplits.add(split);

        context.callAsync(
                () -> true,
                (success, error) -> {
                    if (success && context.registeredReaders().size() > 0) {
                        assignSplits();
                    }
                },
                0,
                10000);
    }

    private void assignSplits() {
        List<Integer> readers = new ArrayList<>(context.registeredReaders().keySet());
        if (readers.isEmpty()) {
            return;
        }

        List<SqsSourceSplit> pendingSplits = new ArrayList<>(assignedSplits);
        assignedSplits.clear();

        for (int i = 0; i < pendingSplits.size(); i++) {
            int readerIndex = i % readers.size();
            context.assignSplit(pendingSplits.get(i), readers.get(readerIndex));
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
    }

    @Override
    public void addSplitsBack(List<SqsSourceSplit> splits, int subtaskId) {
        assignedSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        assignSplits();
    }

    @Override
    public SqsSourceSplitState snapshotState(long checkpointId) throws Exception {
        return new SqsSourceSplitState(assignedSplits);
    }

    @Override
    public void close() throws IOException {
        if (sqsClient != null) {
            sqsClient.close();
        }
    }
}

class SqsSourceReader<OUT> implements SourceReader<OUT, SqsSourceSplit> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SqsSourceReader.class);
    private static final int SQS_MAX_BATCH_SIZE = 10;

    private final String regionName;
    private final DeserializationSchema<OUT> deserializationSchema;
    private final SourceReaderContext readerContext;
    private final int batchSize;
    private final long pollInterval;
    private final Map<String, List<DeleteMessageBatchRequestEntry>> queueUrlEntriesToDeleteMap;
    private final int visibilityTimeoutSeconds;

    private SqsClient sqsClient;
    private final Queue<Message> messageQueue;
    private final List<SqsSourceSplit> assignedSplits;
    private boolean noMoreSplits = false;

    public SqsSourceReader(
            String regionName,
            DeserializationSchema<OUT> deserializationSchema,
            int batchSize,
            int visibilityTimeoutSeconds,
            int pollInterval,
            SourceReaderContext readerContext) {
        this.regionName = regionName;
        this.deserializationSchema = deserializationSchema;
        this.readerContext = readerContext;
        this.batchSize = batchSize;
        this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
        this.pollInterval = pollInterval;
        this.messageQueue = new ArrayBlockingQueue<>(10000);
        this.assignedSplits = new ArrayList<>();
        this.queueUrlEntriesToDeleteMap = new HashMap<>();
    }

    @Override
    public void start() {
        sqsClient = SqsClient.builder()
                .region(Region.of(regionName))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
    }

    @Override
    public void addSplits(List<SqsSourceSplit> splits) {
        assignedSplits.addAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        noMoreSplits = true;
    }

    @Override
    public List<SqsSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(assignedSplits);
    }

    @Override
    public void close() throws Exception {
        if (sqsClient != null) {
            sqsClient.close();
        }
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (!messageQueue.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.supplyAsync(() -> {
            if (assignedSplits.isEmpty()) {
                return null;
            }

            try {
                for (SqsSourceSplit split : assignedSplits) {
                    ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                            .visibilityTimeout(visibilityTimeoutSeconds)
                            .queueUrl(split.getQueueUrl())
                            .maxNumberOfMessages(batchSize)
                            .waitTimeSeconds((int) (pollInterval / 1000))
                            .build();

                    ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
                    List<Message> messages = response.messages();
                    if (!messages.isEmpty()) {
                        messageQueue.addAll(messages);

                        List<DeleteMessageBatchRequestEntry> entries = new ArrayList<>();
                        for (Message message : messages) {
                            entries.add(DeleteMessageBatchRequestEntry.builder()
                                    .id(message.messageId())
                                    .receiptHandle(message.receiptHandle())
                                    .build());
                        }

                        if (!queueUrlEntriesToDeleteMap.containsKey(split.getQueueUrl())) {
                            queueUrlEntriesToDeleteMap.put(split.getQueueUrl(), entries);
                        } else {
                            queueUrlEntriesToDeleteMap.get(split.getQueueUrl()).addAll(entries);
                        }
                    }
                }
            } catch (Exception e) {
                readerContext.sendSourceEventToCoordinator(
                        new SqsSourceReaderEvent("Error polling SQS: " + e.getMessage()));
            }
            return null;
        });
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.info("Deleting SQS messages for successful checkpoint {}", checkpointId);
        for (Map.Entry<String, List<DeleteMessageBatchRequestEntry>> entry : queueUrlEntriesToDeleteMap.entrySet()) {
            List<List<DeleteMessageBatchRequestEntry>> batches = Lists.partition(entry.getValue(), SQS_MAX_BATCH_SIZE);
            for (List<DeleteMessageBatchRequestEntry> batch : batches) {
                DeleteMessageBatchRequest deleteRequest = DeleteMessageBatchRequest.builder()
                        .queueUrl(entry.getKey())
                        .entries(batch)
                        .build();

                sqsClient.deleteMessageBatch(deleteRequest);
            }
        }
        queueUrlEntriesToDeleteMap.clear();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) {
        Message message = messageQueue.poll();
        if (message == null) {
            if (noMoreSplits && assignedSplits.isEmpty()) {
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        try {
            byte[] messageData = message.body().getBytes();
            OUT result = deserializationSchema.deserialize(messageData);
            output.collect(result);
            return InputStatus.MORE_AVAILABLE;
        } catch (Exception e) {
            readerContext.sendSourceEventToCoordinator(
                    new SqsSourceReaderEvent("Error deserializing message: " + e.getMessage()));
            return InputStatus.MORE_AVAILABLE;
        }
    }
}

/**
 * Event class for reader to coordinator communication.
 */
class SqsSourceReaderEvent implements SourceEvent, Serializable {
    private final String message;

    public SqsSourceReaderEvent(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
