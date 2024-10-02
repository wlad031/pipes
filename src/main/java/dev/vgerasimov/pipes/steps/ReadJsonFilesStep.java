package dev.vgerasimov.pipes.steps;

import static org.apache.commons.lang3.exception.ExceptionUtils.rethrow;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vgerasimov.pipes.BaseStep;
import dev.vgerasimov.pipes.Step;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import org.springframework.util.function.ThrowingFunction;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class ReadJsonFilesStep extends BaseStep implements Step {

  private final ObjectMapper objectMapper = new ObjectMapper();

  protected ReadJsonFilesStep(String name) {
    super(name);
  }

  @SuppressWarnings("unchecked")
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

    var rawFiles = payload.get("files");
    if (rawFiles == null) {
      log.warn("Payload doesn't contain files");
      return Flux.empty();
    }

    return getPaths((Collection<String>) rawFiles)
        .publishOn(Schedulers.boundedElastic())
        .mapNotNull(
            nullIfThrows(
                Files::readString, IOException.class, t -> log.error("Cannot read file", t)))
        .filter(Objects::nonNull)
        .mapNotNull(
            nullIfThrows(
                content -> objectMapper.readValue(content, Map.class),
                IOException.class,
                t -> log.error("Cannot parse file", t)))
        .filter(Objects::nonNull)
        .map(map -> (Map<String, Object>) map);
  }

  private Flux<Path> getPaths(Collection<String> rawFiles) {
    try {
      var result = new ArrayList<Path>();

      var queue = new ArrayDeque<>(rawFiles);
      while (!queue.isEmpty()) {
        var file = queue.removeFirst();
        Path path = Paths.get(file);
        if (!Files.exists(path)) {
          log.warn("File {} doesn't exist", path);
          continue;
        }
        if (Files.isDirectory(path)) {
          var filesInDirectory =
              Files.walk(path).filter(Files::isRegularFile).map(Path::toString).toList();
          log.info("Found {} files in directory {}", filesInDirectory.size(), path);
          queue.addAll(filesInDirectory);
          continue;
        }
        result.add(path);
      }

      return Flux.fromIterable(result);
    } catch (IOException e) {
      return Flux.error(e);
    }
  }

  // TODO: Move to a common package
  @SuppressWarnings("unchecked")
  private static <T, S, X extends Throwable> Function<T, S> nullIfThrows(
      ThrowingFunction<T, S> function, Class<X> exceptionClass, Consumer<X> consumer) {
    return t -> {
      try {
        return function.apply(t);
      } catch (Throwable e) {
        if (exceptionClass.isAssignableFrom(e.getClass())) {
          consumer.accept((X) e);
          return null;
        } else {
          return rethrow(e);
        }
      }
    };
  }
}
