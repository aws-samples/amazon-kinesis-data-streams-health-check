package com.myorg;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awscdk.*;
import software.amazon.awscdk.services.cloudwatch.*;
import software.amazon.awscdk.services.cloudwatch.actions.SnsAction;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleProps;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.events.targets.LambdaFunctionProps;
import software.amazon.awscdk.services.iam.*;
import software.amazon.awscdk.services.kinesis.Stream;
import software.amazon.awscdk.services.kinesis.StreamEncryption;
import software.amazon.awscdk.services.kinesis.StreamMode;
import software.amazon.awscdk.services.kms.Alias;
import software.amazon.awscdk.services.kms.AliasProps;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSource;
import software.amazon.awscdk.services.lambda.eventsources.KinesisEventSourceProps;
import software.amazon.awscdk.services.s3.assets.AssetOptions;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sns.TopicProps;
import software.constructs.Construct;

import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static software.amazon.awscdk.BundlingOutput.ARCHIVED;
import static software.amazon.awscdk.services.cloudwatch.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD;
import static software.amazon.awscdk.services.cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD;
import static software.amazon.awscdk.services.cloudwatch.TreatMissingData.BREACHING;
import static software.amazon.awscdk.services.iam.Effect.ALLOW;
import static software.amazon.awscdk.services.lambda.Architecture.X86_64;
import static software.amazon.awscdk.services.logs.RetentionDays.ONE_DAY;

public class KinesisHealthCheckStack extends Stack {
    public KinesisHealthCheckStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    @lombok.SneakyThrows
    public KinesisHealthCheckStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        ObjectMapper objectMapper = new ObjectMapper();

        Stack currentStack = Stack.of(this);

        CfnParameter healthCheckStreamName = CfnParameter.Builder.create(this, "healthcheck-stream-name").description("HealthCheck Kinesis Stream Name").type("String").defaultValue("health-check-stream").build();

        Role hcRole = new Role(this, "HealthCheck-Lambda-Role", RoleProps.builder()
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .path("/")
                .build());
        hcRole.addManagedPolicy(ManagedPolicy.fromManagedPolicyArn(this, "kinesis-policy", "arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole"));
        hcRole.addManagedPolicy(ManagedPolicy.fromManagedPolicyArn(this, "xray-policy", "arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess"));
        hcRole.addToPolicy(new PolicyStatement(PolicyStatementProps.builder()
                .effect(ALLOW)
                .actions(singletonList("cloudwatch:PutMetricData"))
                .resources(singletonList("*"))
                .build()));
        hcRole.addToPolicy(new PolicyStatement(PolicyStatementProps.builder()
                .effect(ALLOW)
                .actions(asList("kinesis:PutRecord", "kinesis:PutRecords", "kinesis:DescribeStream"))
                .resources(singletonList("arn:aws:kinesis:*:" + currentStack.getAccount() + ":stream/" + healthCheckStreamName.getValueAsString()))
                .build()));

        Function kinesisHealthCheckProducerFunction = new Function(this, "Kinesis-HealthCheck-Producer", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .description("Kinesis Health Check Producer Lambda")
                .code(Code.fromAsset("../kinesis-healthcheck-producer-function", AssetOptions.builder()
                        .bundling(BundlingOptions.builder()
                                .command(asList(
                                        "/bin/sh",
                                        "-c",
                                        "cd /asset-input/HealthCheckProducerFunction && mvn clean install " +
                                                "&& cp target/kinesis-healthcheck-producer-function-1.0.jar /asset-output/"
                                ))
                                .image(Runtime.JAVA_11.getBundlingImage())
                                .volumes(singletonList(
                                        // Mount local .m2 repo to avoid download all the dependencies again inside the container
                                        DockerVolume.builder()
                                                .hostPath(System.getProperty("user.home") + "/.m2/")
                                                .containerPath("/root/.m2/")
                                                .build()
                                ))
                                .user("root")
                                .outputType(ARCHIVED)
                                .build())
                        .build()))
                .handler("app.HealthCheckProducerHandler::handleRequest")
                .memorySize(512)
                .timeout(Duration.seconds(15))
                .logRetention(ONE_DAY)
                .architecture(X86_64)
                .environment(singletonMap("JAVA_TOOL_OPTIONS", "-XX:+TieredCompilation -XX:TieredStopAtLevel=1"))
                .role(hcRole)
                .build());

        Map<String, String> inputMap = new HashMap<>();
        inputMap.put("type", "KinesisHealthCheckCanary");
        inputMap.put("streamName", healthCheckStreamName.getValueAsString());
        Rule eventBridgeRule = new Rule(this, "schedule-rule", RuleProps.builder()
                .enabled(true)
                .description("Rule to Trigger HealthCheck Producer")
                .ruleName("KinesisHeartbeatTriggerRule")
                .schedule(Schedule.rate(Duration.minutes(1)))
                .targets(singletonList(new LambdaFunction(kinesisHealthCheckProducerFunction, LambdaFunctionProps.builder()
                        .event(RuleTargetInput.fromObject(inputMap))
                        .build())))
                .build());

        Function kinesisHealthCheckConsumerFunction = new Function(this, "Kinesis-HealthCheck-Consumer", FunctionProps.builder()
                .runtime(Runtime.JAVA_11)
                .description("Kinesis Health Check Consumer Lambda")
                .code(Code.fromAsset("../kinesis-healthcheck-consumer-function", AssetOptions.builder()
                        .bundling(BundlingOptions.builder()
                                .command(asList(
                                        "/bin/sh",
                                        "-c",
                                        "cd /asset-input/HealthCheckConsumerFunction && mvn clean install " +
                                                "&& cp target/kinesis-healthcheck-consumer-function-1.0.jar /asset-output/"
                                ))
                                .image(Runtime.JAVA_11.getBundlingImage())
                                .volumes(singletonList(
                                        // Mount local .m2 repo to avoid download all the dependencies again inside the container
                                        DockerVolume.builder()
                                                .hostPath(System.getProperty("user.home") + "/.m2/")
                                                .containerPath("/root/.m2/")
                                                .build()
                                ))
                                .user("root")
                                .outputType(ARCHIVED)
                                .build())
                        .build()))
                .handler("app.HealthCheckConsumerHandler::handleRequest")
                .memorySize(512)
                .timeout(Duration.seconds(15))
                .logRetention(ONE_DAY)
                .architecture(X86_64)
                .environment(singletonMap("JAVA_TOOL_OPTIONS", "-XX:+TieredCompilation -XX:TieredStopAtLevel=1"))
                .role(hcRole)
                .events(singletonList(new KinesisEventSource(
                        Stream.Builder.create(this, "health-check-stream")
                            .streamName(healthCheckStreamName.getValueAsString())
                            .streamMode(StreamMode.ON_DEMAND)
                            .encryption(StreamEncryption.KMS)
                            .encryptionKey(Alias.fromAliasName(this, "kinesis-key", "alias/aws/kinesis"))
                            .build(),
                        KinesisEventSourceProps.builder()
                                .batchSize(1)
                                .bisectBatchOnError(false)
                                .enabled(true)
                                .startingPosition(StartingPosition.LATEST)
                                .build())
                        ))
                .build());

        Metric hcMetric = new Metric(MetricProps.builder()
                .namespace("KinesisServiceHealthCheck")
                .metricName("HealthCheckSinceSeconds")
                .dimensionsMap(singletonMap("StreamName", healthCheckStreamName.getValueAsString()))
                .statistic("max")
                .period(Duration.minutes(1))
                .build());
        Alarm cwAlarm = Alarm.Builder.create(this, "kinesis-hc-alarm")
                .alarmDescription("Alarm for monitoring the Health of Kinesis Data Streams Producing & Consuming")
                .alarmName("Kinesis-Data-Streams-HealthCheck-Alarm")
                .threshold(1)
                .datapointsToAlarm(1)
                .comparisonOperator(GREATER_THAN_THRESHOLD)
                .actionsEnabled(true)
                .treatMissingData(BREACHING)
                .metric(hcMetric)
                .evaluationPeriods(1)
                .build();
        cwAlarm.addAlarmAction(new SnsAction(new Topic(this, "kds-hc-topic", TopicProps.builder().topicName("kinesis-healthcheck-alarm-topic").build())));

        new CfnOutput(this, "Kinesis-HealthCheck-Role-Arn", CfnOutputProps.builder()
                .description("Kinesis Health Check Role")
                .value(hcRole.getRoleArn())
                .build());

        new CfnOutput(this, "Kinesis-HealthCheck-EventBridge-Rule-Arn", CfnOutputProps.builder()
                .description("Kinesis Health Check EventBridge Rule")
                .value(eventBridgeRule.getRuleArn())
                .build());

        new CfnOutput(this, "Kinesis-HealthCheck-Producer-Arn", CfnOutputProps.builder()
                .description("Kinesis Health Check Producer Lambda")
                .value(kinesisHealthCheckProducerFunction.getFunctionArn())
                .build());

        new CfnOutput(this, "Kinesis-HealthCheck-Consumer-Arn", CfnOutputProps.builder()
                .description("Kinesis Health Check Consumer Lambda")
                .value(kinesisHealthCheckConsumerFunction.getFunctionArn())
                .build());
    }
}
