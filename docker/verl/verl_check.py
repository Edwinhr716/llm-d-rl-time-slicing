"""Build-time check for the verl image: the verl fork and the timeslice plugin load together."""

import importlib.metadata as md

import google.protobuf as pb
import grpc
import timeslice  # noqa: F401
import verl  # loads verl.plugins, which registers the timeslice trainer
from timeslice_verl.trainer import TimesliceFullyAsyncTrainer
from verl.experimental.fully_async_policy.fully_async_trainer import get_trainer_cls

assert any(ep.name == "timeslice" for ep in md.entry_points(group="verl.plugins")), "verl.plugins entry point missing"
assert get_trainer_cls("timeslice") is TimesliceFullyAsyncTrainer, "trainer_name=timeslice does not resolve"
print("OK verl", verl.__version__, verl.__file__, "grpcio", grpc.__version__, "protobuf", pb.__version__)
