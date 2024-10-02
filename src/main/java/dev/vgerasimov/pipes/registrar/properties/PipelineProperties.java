package dev.vgerasimov.pipes.registrar.properties;

import java.util.List;

public record PipelineProperties(
    String name,
    List<String> steps
) {
}
