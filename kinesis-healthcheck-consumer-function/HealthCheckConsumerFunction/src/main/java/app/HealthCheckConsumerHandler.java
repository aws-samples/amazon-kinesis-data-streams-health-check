package app;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.Dimension;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;

import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

/**
 * Handler for requests to Lambda function.
 */
public class HealthCheckConsumerHandler implements RequestHandler<KinesisEvent, String> {

    private static final Logger logger = LoggerFactory.getLogger(HealthCheckConsumerHandler.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final CharsetDecoder utf8Decoder = StandardCharsets.UTF_8.newDecoder();
    private CloudWatchClient cw = null;

    public HealthCheckConsumerHandler() {

        cw = CloudWatchClient.builder()
                .region(Region.of(System.getenv("AWS_REGION")))
                .httpClient(ApacheHttpClient.create())
                .build();
    }

    public HealthCheckConsumerHandler(CloudWatchClient cw) {

        this.cw = cw;
    }

    @Override
    public String handleRequest(KinesisEvent input, Context context) {
        try {
            logger.info("Input Event: {}", objectMapper.writeValueAsString(input));
            String streamName = getStreamName(input.getRecords().get(0));
            try {
                String lastHealthCheckTimestamp;
                for (KinesisEvent.KinesisEventRecord record : input.getRecords()) {
                    String actualDataString = utf8Decoder.decode(record.getKinesis().getData()).toString();
                    logger.info("Actual Record Data: {}", actualDataString);
                    lastHealthCheckTimestamp = toISO8601UTC(record.getKinesis().getApproximateArrivalTimestamp());
//                    lastHealthCheckTimestamp = objectMapper.readTree(actualDataString).at("/currentInstant").asText();
                    logger.info("LastHealthCheckTimestamp: {}", lastHealthCheckTimestamp);

                    long healthCheckSinceSeconds = Duration.between(Instant.parse(lastHealthCheckTimestamp), Instant.now()).toSeconds();
                    MetricDatum healthCheckSinceMetricData = MetricDatum.builder()
                            .metricName("HealthCheckSinceSeconds")
                            .dimensions(Dimension.builder()
                                    .name("StreamName")
                                    .value(streamName)
                                    .build())
                            .value((double) healthCheckSinceSeconds)
                            .build();
                    cw.putMetricData(PutMetricDataRequest.builder()
                            .namespace("KinesisServiceHealthCheck")
                            .metricData(Collections.singletonList(healthCheckSinceMetricData))
                            .build());
                    logger.info("Last HealthCheck was received {} seconds ago", healthCheckSinceSeconds);
                    return String.valueOf(healthCheckSinceSeconds);
                }
            } catch (Exception e) {
                logger.error("Error occurred", e);
            }

        } catch (JsonProcessingException e) {
            logger.error("Error occurred: {}", input, e);
        }
        return null;
    }

    private String getStreamName(KinesisEvent.KinesisEventRecord record) {
        return record.getEventSourceARN().split(":")[5].split("/")[1];
    }


    private String toISO8601UTC(Date date) {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        df.setTimeZone(tz);
        return df.format(date);
    }
}
