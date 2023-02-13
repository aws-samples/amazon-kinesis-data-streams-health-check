package app;

import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class HandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private CloudWatchClient cloudWatchClient;
    private HealthCheckConsumerHandler handler;

    @BeforeEach
    void init() {
        cloudWatchClient = mock(CloudWatchClient.class);
        handler = new HealthCheckConsumerHandler(cloudWatchClient);
    }

    @Test
    void shouldHandleKinesisEvent() throws IOException {
        // Given
        KinesisEvent kinesisEvent = readJsonEvent("kinesis-event.json", KinesisEvent.class);

        // When
        String healthCheckSinceSeconds = handler.handleRequest(kinesisEvent, null);

        // Then
        assertNotNull(healthCheckSinceSeconds);
        verify(cloudWatchClient).putMetricData(any(PutMetricDataRequest.class));
    }

    @Test
    void shouldHandleFailure() throws IOException {
        // Given
        KinesisEvent kinesisEvent = readJsonEvent("kinesis-event.json", KinesisEvent.class);
        when(cloudWatchClient.putMetricData(any(PutMetricDataRequest.class))).thenThrow(new RuntimeException("some-exception"));

        // When & Then
        String healthCheckSinceSeconds = handler.handleRequest(kinesisEvent, null);

        // Then
        assertNull(healthCheckSinceSeconds);
        verify(cloudWatchClient).putMetricData(any(PutMetricDataRequest.class));
    }

    @SneakyThrows
    KinesisEvent readJsonEvent(String fileName, Class clazz){
        ClassLoader classLoader = getClass().getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return objectMapper.readValue(file, KinesisEvent.class);
    }

}
