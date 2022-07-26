package com.example.demo;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DemoApplication {

  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "b-1.prodotakafka.rceie5.c3.kafka.ap-southeast-1.amazonaws.com:9098,b-2.prodotakafka.rceie5.c3.kafka.ap-southeast-1.amazonaws.com:9098");
    properties.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    properties.setProperty("sasl.mechanism", "AWS_MSK_IAM");
    properties.setProperty("sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;");
    properties.setProperty("sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

    System.out.println("Before topic created");

    try (AdminClient client = AdminClient.create(properties)) {
      System.out.println("Before client.createTopics");
      final CreateTopicsResult res = client.createTopics(
          Collections.singletonList(
              new NewTopic("foo", 1, (short) 1)
          )
      );
      res.all().get(5, TimeUnit.SECONDS);
      System.out.println("topic created");
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      System.out.println(e);
    }
    System.out.println("After topic created");
  }

}
