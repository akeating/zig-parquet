"""Verify that zig-parquet-written files are readable by PyArrow."""

import struct
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import pyarrow.parquet as pq

INTEROP_DIR = Path(__file__).parent / "interop"


def interop_path(name: str) -> Path:
    path = INTEROP_DIR / name
    if not path.exists():
        pytest.skip(f"{path} not found — run 'zig build test' first")
    return path


def _as_str(v):
    return v.decode("utf-8") if isinstance(v, bytes) else v


def _to_bytes(v):
    if v is None:
        return None
    return v.bytes if hasattr(v, "bytes") else bytes(v)


def _normalize_ts(ts):
    if ts is None:
        return None
    if hasattr(ts, "astimezone") and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


def _decode_interval(raw):
    if raw is None:
        return None
    b = bytes(raw)
    months = struct.unpack_from("<I", b, 0)[0]
    days = struct.unpack_from("<I", b, 4)[0]
    millis = struct.unpack_from("<I", b, 8)[0]
    return (months, days, millis)


def _wkb_point_xy(raw):
    x = struct.unpack_from("<d", raw, 5)[0]
    y = struct.unpack_from("<d", raw, 13)[0]
    return x, y


# ── ENUM ──────────────────────────────────────────────────────────────────────


class TestEnum:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("enum.parquet"))
        self.values = self.table.column("color").to_pylist()

    def test_row_count(self):
        assert len(self.values) == 5

    def test_values(self):
        assert _as_str(self.values[0]) == "RED"
        assert _as_str(self.values[1]) == "GREEN"
        assert _as_str(self.values[2]) == "BLUE"
        assert self.values[3] is None
        assert _as_str(self.values[4]) == "RED"

    def test_physical_type(self):
        schema_text = str(pq.ParquetFile(interop_path("enum.parquet")).schema)
        assert "BYTE_ARRAY" in schema_text or "binary" in schema_text


# ── BSON ──────────────────────────────────────────────────────────────────────


class TestBson:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("bson.parquet"))
        self.values = self.table.column("data").to_pylist()

    def test_row_count(self):
        assert len(self.values) == 4

    def test_values(self):
        assert isinstance(self.values[0], bytes)
        assert len(self.values[0]) == 12  # {"x": 1}
        assert len(self.values[1]) == 5  # {}
        assert self.values[2] is None
        assert len(self.values[3]) == 15  # {"s": "hi"}


# ── UUID ──────────────────────────────────────────────────────────────────────


class TestUuid:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("uuid.parquet"))
        self.values = self.table.column("id").to_pylist()

    def test_row_count(self):
        assert len(self.values) == 4

    def test_value_0(self):
        expected = bytes(
            [0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78,
             0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78]
        )
        assert _to_bytes(self.values[0]) == expected

    def test_value_1_all_zeros(self):
        assert _to_bytes(self.values[1]) == bytes(16)

    def test_value_2_null(self):
        assert self.values[2] is None

    def test_value_3(self):
        expected = bytes(
            [0xA1, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x78, 0x90,
             0xAB, 0xCD, 0xEF, 0x12, 0x34, 0x56, 0x78, 0x90]
        )
        assert _to_bytes(self.values[3]) == expected


# ── Interval ──────────────────────────────────────────────────────────────────


class TestInterval:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("interval.parquet"))
        self.values = self.table.column("duration").to_pylist()

    def test_row_count(self):
        assert len(self.values) == 4

    def test_values(self):
        assert _decode_interval(self.values[0]) == (1, 15, 3_600_000)
        assert _decode_interval(self.values[1]) == (0, 0, 0)
        assert self.values[2] is None
        assert _decode_interval(self.values[3]) == (12, 365, 86_400_000)


# ── Int96 Timestamp ───────────────────────────────────────────────────────────


class TestInt96Timestamp:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("int96_timestamp.parquet"))
        self.values = self.table.column("ts").to_pylist()

    def test_row_count(self):
        assert len(self.values) == 4

    def test_value_0(self):
        expected = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        assert _normalize_ts(self.values[0]) == expected

    def test_value_1_epoch(self):
        expected = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert _normalize_ts(self.values[1]) == expected

    def test_value_2_null(self):
        assert self.values[2] is None

    def test_value_3(self):
        expected = datetime(2000, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        assert _normalize_ts(self.values[3]) == expected


# ── Geometry ──────────────────────────────────────────────────────────────────


class TestGeometry:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("geometry.parquet"))
        self.values = self.table.column("geom").to_pylist()

    def test_row_count(self):
        assert len(self.values) == 3

    def test_point_0(self):
        assert len(self.values[0]) == 21  # WKB Point
        x, y = _wkb_point_xy(self.values[0])
        assert x == pytest.approx(1.0)
        assert y == pytest.approx(2.0)

    def test_point_1(self):
        assert len(self.values[1]) == 21
        x, y = _wkb_point_xy(self.values[1])
        assert x == pytest.approx(45.0)
        assert y == pytest.approx(10.0)

    def test_value_2_null(self):
        assert self.values[2] is None


# ── Geography ─────────────────────────────────────────────────────────────────


class TestGeography:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("geography.parquet"))
        self.values = self.table.column("geog").to_pylist()

    def test_row_count(self):
        assert len(self.values) == 3

    def test_point_0(self):
        assert len(self.values[0]) == 21
        x, y = _wkb_point_xy(self.values[0])
        assert x == pytest.approx(100.0)
        assert y == pytest.approx(-40.0)

    def test_point_1(self):
        assert len(self.values[1]) == 21
        x, y = _wkb_point_xy(self.values[1])
        assert x == pytest.approx(-75.0)
        assert y == pytest.approx(41.0)

    def test_value_2_null(self):
        assert self.values[2] is None


# ── Primitives ────────────────────────────────────────────────────────────────


class TestPrimitives:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("primitives.parquet"))

    def test_row_count(self):
        assert len(self.table) == 4

    def test_bool_col(self):
        vals = self.table.column("bool_col").to_pylist()
        assert vals == [True, False, None, True]

    def test_int32_col(self):
        vals = self.table.column("int32_col").to_pylist()
        assert vals == [42, -100, 2_147_483_647, None]

    def test_int64_col(self):
        vals = self.table.column("int64_col").to_pylist()
        assert vals == [1_000_000_000_000, None, -9_223_372_036_854_775_808, 0]

    def test_float_col(self):
        vals = self.table.column("float_col").to_pylist()
        assert vals[0] == pytest.approx(3.14, abs=0.01)
        assert vals[1] == pytest.approx(-2.5, abs=0.01)
        assert vals[2] is None
        assert vals[3] == 0.0

    def test_double_col(self):
        vals = self.table.column("double_col").to_pylist()
        assert vals[0] == pytest.approx(1.23456789012345, abs=1e-12)
        assert vals[1] == pytest.approx(-99.99, abs=0.001)
        assert vals[3] is None

    def test_string_col(self):
        vals = self.table.column("string_col").to_pylist()
        assert vals == ["hello", "world", None, ""]

    def test_binary_col(self):
        vals = self.table.column("binary_col").to_pylist()
        assert vals == [b"\x00\x01\x02\x03", None, b"\xff\xfe", b""]


# ── Small Ints ────────────────────────────────────────────────────────────────


class TestSmallInts:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("small_ints.parquet"))

    def test_row_count(self):
        assert len(self.table) == 4

    def test_i8_col(self):
        vals = self.table.column("i8_col").to_pylist()
        assert vals == [-128, 127, 0, None]

    def test_u8_col(self):
        vals = self.table.column("u8_col").to_pylist()
        assert vals == [0, 255, None, 128]

    def test_u64_col(self):
        vals = self.table.column("u64_col").to_pylist()
        assert vals == [0, 9_223_372_036_854_775_807, None, 1]


# ── Temporal ──────────────────────────────────────────────────────────────────


class TestTemporal:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("temporal.parquet"))

    def test_row_count(self):
        assert len(self.table) == 4

    def test_date_col(self):
        vals = self.table.column("date_col").to_pylist()
        assert vals[0] == date(2024, 1, 15)
        assert vals[1] == date(1970, 1, 1)
        assert vals[2] is None
        assert vals[3] == date(2000, 6, 15)

    def test_ts_millis(self):
        vals = self.table.column("ts_millis").to_pylist()
        expected = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        assert _normalize_ts(vals[0]) == expected
        assert vals[2] is None

    def test_time_millis(self):
        vals = self.table.column("time_millis").to_pylist()
        assert vals[0] == time(10, 30, 0)
        assert vals[1] == time(0, 0, 0)
        assert vals[2] is None
        assert vals[3] == time(12, 0, 0)


# ── Decimal ───────────────────────────────────────────────────────────────────


class TestDecimal:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("decimal.parquet"))

    def test_row_count(self):
        assert len(self.table) == 4

    def test_dec_9_2(self):
        vals = self.table.column("dec_9_2").to_pylist()
        assert vals[0] == Decimal("123.45")
        assert vals[1] == Decimal("-999.99")
        assert vals[2] == Decimal("0.01")
        assert vals[3] is None

    def test_dec_18_4(self):
        vals = self.table.column("dec_18_4").to_pylist()
        assert vals[0] == Decimal("12345678901234.5678")
        assert vals[1] is None
        assert vals[2] == Decimal("0.0001")
        assert vals[3] == Decimal("1.0000")


# ── Float16 ───────────────────────────────────────────────────────────────────


class TestFloat16:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("float16.parquet"))
        self.values = self.table.column("f16_col").to_pylist()

    def test_row_count(self):
        assert len(self.table) == 4

    def test_value_0(self):
        v0 = self.values[0]
        assert v0 is not None
        if isinstance(v0, (bytes, bytearray)):
            assert len(v0) == 2
        else:
            assert float(v0) == pytest.approx(1.5, abs=0.01)

    def test_value_3_null(self):
        assert self.values[3] is None


# ── Nested ────────────────────────────────────────────────────────────────────


class TestNested:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.table = pq.read_table(interop_path("nested.parquet"))

    def test_row_count(self):
        assert len(self.table) == 4

    def test_int_list(self):
        vals = self.table.column("int_list").to_pylist()
        assert vals[0] == [1, 2, 3]
        assert vals[1] == [4, 5]
        assert vals[2] is None
        assert vals[3] == []

    def test_point_struct(self):
        vals = self.table.column("point").to_pylist()
        assert vals[0] == {"x": 10, "y": 11}
        assert vals[1] == {"x": 20, "y": 21}
        assert vals[3] == {"x": 30, "y": 31}
