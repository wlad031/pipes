package dev.vgerasimov.pipes;

import java.util.Map;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

public interface Step extends Function<Map<String, Object>, Flux<Map<String, Object>>> {

  String getName();

  default Step andThen(Step that) {
    return new AndThen(this, that);
  }

  static Step error(Throwable error) {
    return new Error(error);
  }

  @RequiredArgsConstructor
  class AndThen implements Step {
    private final Step first;
    private final Step second;

    @Override
    public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
      return first.apply(payload).flatMap(second);
    }

    @Override
    public String getName() {
      return String.format("(%s andThen %s)", first.getName(), second.getName());
    }
  }

  @RequiredArgsConstructor
  class Error implements Step {
    private final Throwable error;

    @Override
    public Flux<Map<String, Object>> apply(Map<String, Object> payload) {
      return Flux.error(error);
    }

    @Override
    public String getName() {
      return "error";
    }
  }
}
