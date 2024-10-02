package dev.vgerasimov.pipes;

import static java.util.stream.Collectors.joining;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Flux;

public class PipelineImpl extends BaseStep implements Pipeline, SourceStep {

  protected final List<Step> steps;

  PipelineImpl(ApplicationContext context, String name, List<String> stepNames) {
    super(name);
    this.steps = stepNames.stream().map(context::getBean).map(PipelineImpl::cast).toList();
    log.info(
        "Pipeline initialized with {} steps: {}",
        steps.size(),
        steps.stream().map(Step::getName).collect(joining("\n\t - ", "\n\t - ", "")));
  }

  @Override
  @PostConstruct
  public void runOnPostConstruct() {
    log.info("Running pipeline");
    apply().subscribe();
  }

  @Override
  public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
    if (steps.isEmpty()) {
      log.warn("Pipeline has no steps");
      return Flux.empty();
    } else if (steps.size() == 1) {
      return steps.getFirst().apply(payload);
    } else {
      return steps.stream()
          .reduce(Step::andThen)
          .map(f -> f.apply(payload))
          .orElseGet(() -> Flux.just(payload));
    }
  }

  @Override
  public Flux<Map<String, Object>> apply() {
    if (steps.isEmpty()) {
      log.warn("Pipeline has no steps");
      return Flux.empty();
    } else {
      var first = steps.getFirst();
      if (first instanceof SourceStep source) {
        var result = source.apply();
        if (steps.size() == 1) {
          return result;
        } else {
          return result.flatMap(
              steps.subList(1, steps.size()).stream()
                  .reduce(Step::andThen)
                  .orElseGet(
                      () -> Step.error(new IllegalStateException("Unexpected end of steps"))));
        }
      } else {
        log.warn("Pipeline {} has no source step, running it with empty payload", getName());
        return apply(new HashMap<>());
      }
    }
  }

  private static Step cast(Object obj) {
    if (obj instanceof Step step) {
      return step;
    } else {
      throw new IllegalStateException(String.format("Bean %s is not a step", obj));
    }
  }
}
