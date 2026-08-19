import pytest

from timeslice.snapshot_agent import MemoryRegion, memory_regions_config
from timeslice.snapshot_agent import snapshot_agent_pb2


class TestMemoryRegionFromSpec:
    @pytest.mark.parametrize(
        "spec,expected",
        [
            ("123:0x7f00:1024", MemoryRegion(pid=123, address=0x7F00, size_bytes=1024)),
            (
                "123:139637976727552:1073741824",
                MemoryRegion(pid=123, address=139637976727552, size_bytes=1073741824),
            ),
            ("1:0x0:1", MemoryRegion(pid=1, address=0, size_bytes=1)),
            # hex size accepted too
            ("42:0xABCD:0x100", MemoryRegion(pid=42, address=0xABCD, size_bytes=256)),
        ],
    )
    def test_valid_specs(self, spec, expected):
        assert MemoryRegion.from_spec(spec) == expected

    @pytest.mark.parametrize(
        "spec",
        [
            "invalid-format",
            "123:0x7f00",  # missing size
            "123:0x7f00:1024:extra",
            "abc:0x7f00:1024",  # non-numeric pid
            "123:zz:1024",  # bad address
            "123:0x7f00:big",  # bad size
            "",
        ],
    )
    def test_invalid_specs(self, spec):
        with pytest.raises(ValueError):
            MemoryRegion.from_spec(spec)

    @pytest.mark.parametrize(
        "kwargs",
        [
            dict(pid=0, address=1, size_bytes=1),
            dict(pid=-1, address=1, size_bytes=1),
            dict(pid=1, address=-1, size_bytes=1),
            dict(pid=1, address=1, size_bytes=0),
            dict(pid=1, address=1, size_bytes=-5),
        ],
    )
    def test_validation(self, kwargs):
        with pytest.raises(ValueError):
            MemoryRegion(**kwargs)


class TestMemoryRegionsConfig:
    @pytest.mark.parametrize(
        "regions",
        [
            [MemoryRegion(pid=123, address=0x7F00, size_bytes=1024)],
            [(123, 0x7F00, 1024)],
            ["123:0x7f00:1024"],
            # mixed input kinds
            [
                MemoryRegion(pid=123, address=0x7F00, size_bytes=1024),
                (123, 0x8F00, 2048),
                "456:0x9f00:4096",
            ],
        ],
    )
    def test_builds_backend_config(self, regions):
        config = memory_regions_config(regions, snapshot_name="slot-a")
        assert isinstance(config, snapshot_agent_pb2.BackendConfig)
        assert config.WhichOneof("backend") == "memory_regions"
        assert config.memory_regions.snapshot_name == "slot-a"
        assert len(config.memory_regions.regions) == len(regions)
        first = config.memory_regions.regions[0]
        assert first.pid == 123
        assert first.address == 0x7F00
        assert first.size_bytes == 1024

    def test_snapshot_name_defaults_to_empty(self):
        config = memory_regions_config([(1, 0x10, 16)])
        assert config.memory_regions.snapshot_name == ""

    def test_large_address_round_trips(self):
        addr = 139637976727552  # > 2**32: uint64 on the wire
        config = memory_regions_config([(123, addr, 2**33)])
        assert config.memory_regions.regions[0].address == addr
        assert config.memory_regions.regions[0].size_bytes == 2**33
        # serialization round-trip
        parsed = snapshot_agent_pb2.BackendConfig.FromString(
            config.SerializeToString()
        )
        assert parsed.memory_regions.regions[0].address == addr

    @pytest.mark.parametrize("regions", [[], None])
    def test_empty_regions_rejected(self, regions):
        with pytest.raises(ValueError, match="at least one memory region"):
            memory_regions_config(regions)

    @pytest.mark.parametrize(
        "region,exc",
        [
            ("bad-spec", ValueError),
            ((1, 2), ValueError),  # wrong arity
            ((0, 2, 3), ValueError),  # zero pid
            ((1, 2, 0), ValueError),  # zero size
            (12345, TypeError),  # unsupported type
        ],
    )
    def test_invalid_regions_rejected(self, region, exc):
        with pytest.raises(exc):
            memory_regions_config([region])
