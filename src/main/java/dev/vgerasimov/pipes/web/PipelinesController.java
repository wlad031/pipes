package dev.vgerasimov.pipes.web;

import static java.util.stream.Collectors.toMap;

import dev.vgerasimov.pipes.Pipeline;
import jakarta.annotation.PostConstruct;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
@RequestMapping(
    value = "/api/v1/pipe",
    consumes = MediaType.APPLICATION_JSON_VALUE,
    produces = MediaType.APPLICATION_JSON_VALUE)
public class PipelinesController {

  private Map<String, Pipeline> pipelines;

  @Autowired
  PipelinesController(List<Pipeline> pipelines) {
    this.pipelines = pipelines.stream().collect(toMap(Pipeline::getName, Function.identity()));
  }

  @PostConstruct
  void log() {
    this.pipelines.forEach((k, v) -> log.info("Pipeline {} registered as Rest", k));
  }

  @GetMapping
  Flux<Collection<String>> list() {
    return Flux.just(pipelines.keySet());
  }

  @PostMapping("/run/body/{pipeName}")
  Mono<Map<String, Object>> runBody(
      @RequestBody Map<String, Object> request, @PathVariable String pipeName) {
    var pipe = pipelines.get(pipeName);
    if (pipe == null) {
      return Mono.error(new IllegalArgumentException("Pipeline not registered"));
    }
    return pipe.apply(request)
        .collectList()
        .map(
            res ->
                res.stream()
                    .reduce(
                        (m1, m2) -> {
                          m1.putAll(m2);
                          return m1;
                        })
                    .orElseGet(HashMap::new));
  }

  @PostMapping(value = "/run/multipart/{pipeName}", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
  Mono<Map<String, Object>> runMultipart(
      @RequestBody List<MultipartFile> files, @PathVariable String pipeName) {
    var pipe = pipelines.get(pipeName);
    if (pipe == null) {
      return Mono.error(new IllegalArgumentException("Pipeline not registered"));
    }
    return pipe.apply(Map.of("files", files))
        .collectList()
        .map(
            res ->
                res.stream()
                    .reduce(
                        (m1, m2) -> {
                          var map = new HashMap<String, Object>();
                          map.putAll(m1);
                          map.putAll(m2);
                          return map;
                        })
                    .orElseGet(HashMap::new));
  }
}
