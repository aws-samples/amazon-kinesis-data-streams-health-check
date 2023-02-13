package app;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordResponse;

import java.util.Collections;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class HandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private KinesisClient kinesisClient;

    @BeforeEach
    void init() {
        kinesisClient = mock(KinesisClient.class);
    }

    @Test
    void shouldProcessEventSuccessfully() throws JsonProcessingException {

        // Given
        EventBridgeTriggerEvent input = new EventBridgeTriggerEvent("something", "some-stream");
        HealthCheckProducerHandler handler = new HealthCheckProducerHandler(kinesisClient);

        // When
        when(kinesisClient.putRecord(any(PutRecordRequest.class))).thenReturn(PutRecordResponse.builder().build());
        String response = handler.handleRequest(input, null);

        // Then
        Map<String, String> dataMap = Collections.singletonMap("currentInstant", response);
        PutRecordRequest putRecordRequest = PutRecordRequest.builder()
                .streamName(input.getStreamName())
                .partitionKey(response)
                .data(SdkBytes.fromString(objectMapper.writeValueAsString(dataMap), UTF_8))
                .build();
        verify(kinesisClient).putRecord(putRecordRequest);
    }

    @Test
    void shouldHandleFailure() throws JsonProcessingException {
        // Given
        EventBridgeTriggerEvent input = new EventBridgeTriggerEvent("something", "some-stream");
        HealthCheckProducerHandler handler = new HealthCheckProducerHandler(kinesisClient);

        // When
        when(kinesisClient.putRecord(any(PutRecordRequest.class))).thenThrow(new RuntimeException("some-exception"));
        String response = handler.handleRequest(input, null);

        // Then

        verify(kinesisClient).putRecord(any(PutRecordRequest.class));
        assertEquals(null, response);
    }

}
