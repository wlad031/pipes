package dev.vgerasimov.pipes;

import jakarta.annotation.PostConstruct;

public interface Pipeline extends Step {

  @PostConstruct
  void runOnPostConstruct();
}
