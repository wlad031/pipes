package dev.vgerasimov.pipes.registrar.models;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@RequiredArgsConstructor
public class Context {

  @Getter @Setter private boolean kafkaTemplateRegistered;
}
