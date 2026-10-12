"""Verify pipeline order, shared-session ownership, and failure cleanup."""

from contextlib import ExitStack
from pathlib import Path
import unittest
from unittest.mock import Mock, call, patch

import pipeline


def builder_for(spark):
    builder = Mock()
    builder.appName.return_value = builder
    builder.master.return_value = builder
    builder.config.return_value = builder
    builder.getOrCreate.return_value = spark
    return builder


class PipelineTests(unittest.TestCase):
    def steps(self):
        return [
            (pipeline.transform.bronze, "run", "bronze"),
            (pipeline.validation.bronze, "test", "bronze_test"),
            (pipeline.transform.silver, "run", "silver"),
            (pipeline.validation.silver, "test", "silver_test"),
            (pipeline.transform.gold, "run", "gold"),
        ]

    def test_runs_and_validates_all_stages_with_one_session(self):
        spark = Mock()
        builder = builder_for(spark)
        events = Mock()
        with ExitStack() as stack:
            create = stack.enter_context(patch.object(
                pipeline.SparkSession, "Builder", return_value=builder,
            ))
            events.attach_mock(create, "create")
            for module, method, name in self.steps():
                mock = stack.enter_context(patch.object(module, method))
                events.attach_mock(mock, name)
            events.attach_mock(spark.stop, "stop")
            pipeline.run(Path("input"), Path("output"), "local[1]")
        builder.appName.assert_called_once_with("befly-hotel-pipeline")
        builder.master.assert_called_once_with("local[1]")
        builder.getOrCreate.assert_called_once_with()
        spark.sparkContext.setLogLevel.assert_called_once_with("WARN")
        self.assertEqual(events.mock_calls, [
            call.create(),
            call.bronze(spark, Path("input"), Path("output/bronze")),
            call.bronze_test(spark, Path("output/bronze")),
            call.silver(spark, Path("output/bronze"), Path("output/silver")),
            call.silver_test(spark, Path("output/bronze"), Path("output/silver")),
            call.gold(spark, Path("output/silver"), Path("output/gold")),
            call.stop(),
        ])

    def test_stage_or_validation_failure_stops_session_and_later_steps(self):
        for failed_step in range(len(self.steps())):
            with self.subTest(failed_step=failed_step), ExitStack() as stack:
                spark = Mock()
                stack.enter_context(patch.object(
                    pipeline.SparkSession, "Builder",
                    return_value=builder_for(spark),
                ))
                steps = [
                    stack.enter_context(patch.object(module, method))
                    for module, method, _ in self.steps()
                ]
                error = ValueError("step failed")
                steps[failed_step].side_effect = error
                with self.assertRaises(ValueError) as raised:
                    pipeline.run(Path("input"), Path("output"))
                self.assertIs(raised.exception, error)
                spark.stop.assert_called_once_with()
                for index, step in enumerate(steps):
                    self.assertEqual(step.call_count, int(index <= failed_step))


if __name__ == "__main__":
    unittest.main()
