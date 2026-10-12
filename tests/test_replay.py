from __future__ import annotations

from src.utils.replay import release_chunks


def make_sources(tmp_path):
    raw = tmp_path / "raw"
    raw.mkdir()
    s1 = b"".join(f"a{i},1\n".encode() for i in range(7))
    s2 = b"b0,1\nb1,1"  # no trailing newline
    (raw / "s_1.csv").write_bytes(s1)
    (raw / "s_2.csv").write_bytes(s2)
    return str(raw / "s_*.csv"), s1, s2


def test_releases_next_chunks_without_overlap(tmp_path):
    glob, _, _ = make_sources(tmp_path)
    landing = tmp_path / "landing"

    first = release_chunks(glob, str(landing), chunk_rows=3, n_files=2)
    second = release_chunks(glob, str(landing), chunk_rows=3, n_files=2)
    third = release_chunks(glob, str(landing), chunk_rows=3, n_files=2)

    assert first == ["s_1.part-0000.csv", "s_1.part-0001.csv"]
    assert second == ["s_1.part-0002.csv", "s_2.part-0000.csv"]
    assert third == []
    assert sorted(p.name for p in (landing / "incoming").iterdir()) == first + second
    assert list((landing / "_staging").iterdir()) == []


def test_release_all_reproduces_sources_byte_for_byte(tmp_path):
    glob, s1, s2 = make_sources(tmp_path)
    landing = tmp_path / "landing"

    released = release_chunks(glob, str(landing), chunk_rows=3, n_files=0)

    incoming = landing / "incoming"
    assert len(released) == 4
    assert b"".join((incoming / n).read_bytes() for n in released if n.startswith("s_1")) == s1
    assert (incoming / "s_2.part-0000.csv").read_bytes() == s2
