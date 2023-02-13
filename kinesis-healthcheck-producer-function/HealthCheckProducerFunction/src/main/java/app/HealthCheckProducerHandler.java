package app;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Handler for requests to Lambda function.
 */
public class HealthCheckProducerHandler implements RequestHandler<EventBridgeTriggerEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckProducerHandler.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    private KinesisClient kinesisForwarder = null;

    public HealthCheckProducerHandler() {

        this.kinesisForwarder = KinesisClient.builder()
                .region(Region.of(System.getenv("AWS_REGION")))
                .httpClient(ApacheHttpClient.create())
                .build();
    }

    public HealthCheckProducerHandler(KinesisClient kinesisForwarder) {
        this.kinesisForwarder = kinesisForwarder;
    }

    @Override
    public String handleRequest(EventBridgeTriggerEvent input, Context context) {
        try {
            logger.info("Input Event: {}", objectMapper.writeValueAsString(input));
            String currentInstant = Instant.now().toString();
            Map<String, String> dataMap = Collections.singletonMap("currentInstant", currentInstant);
            PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                    .streamName(input.getStreamName())
                    .partitionKey(currentInstant)
                    .data(SdkBytes.fromString(objectMapper.writeValueAsString(dataMap), UTF_8))
                    .build();
            kinesisForwarder.putRecord(putRecordRequest);
            logger.info("Produced HealthCheck Message at {}", currentInstant);
            return currentInstant;
        } catch (Exception e) {
            logger.error("Error occurred: {}", input, e);
        }
        return null;
    }
}
