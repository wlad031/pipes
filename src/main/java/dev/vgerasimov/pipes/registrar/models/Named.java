package dev.vgerasimov.pipes.registrar.models;

public record Named<T>(String name, Class<?> type, T value) {}
