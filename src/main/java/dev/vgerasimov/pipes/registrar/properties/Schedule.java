package dev.vgerasimov.pipes.registrar.properties;

import java.time.Duration;

public sealed interface Schedule permits Schedule.Cron, Schedule.Interval {

  Duration window();

  record Cron(String expression, Duration window) implements Schedule {}

  record Interval(Duration interval, Duration window) implements Schedule {}
}
