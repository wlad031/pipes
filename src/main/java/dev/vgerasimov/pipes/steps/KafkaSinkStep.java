package dev.vgerasimov.pipes.steps;

import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.Step;
import java.util.Map;
import org.springframework.kafka.core.KafkaTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class KafkaSinkStep extends BaseStep implements Step {

  private final KafkaTemplate<String, Map<String, Object>> kafkaTemplate;
  private final String topicName;

  public KafkaSinkStep(
      String name, KafkaTemplate<String, Map<String, Object>> kafkaTemplate, String topicName) {
    super(name);
    this.kafkaTemplate = kafkaTemplate;
    this.topicName = topicName;
  }

  @Override
  public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
    if (payload == null) {
      log.error("Payload is null");
      return Flux.empty();
    }
    if (payload.isEmpty()) {
      log.warn("Payload is empty");
      return Flux.just(payload);
    }
    log.debug("Sending payload: {}", payload);
    return Mono.fromCompletionStage(
            kafkaTemplate.send(topicName, Integer.toString(payload.hashCode()), payload))
        .map(sendResult -> sendResult.getProducerRecord().value())
        .doOnNext(s -> log.debug("Response: {}", s))
        .flux()
        .doOnError(t -> log.error("Got error", t))
        .onErrorResume(t -> Flux.empty());
  }
}
