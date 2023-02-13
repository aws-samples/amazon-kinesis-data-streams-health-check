package com.myorg;

import software.amazon.awscdk.App;
import software.amazon.awscdk.StackProps;

public class KinesisHealthCheckApp {
    public static void main(final String[] args) {
        App app = new App();

        new KinesisHealthCheckStack(app, "KinesisHealthCheckStack", StackProps.builder()
                .build());

        app.synth();
    }
}

