"""清单/块格式纯函数校验：压缩往返、块散列、对象散列、根散列、不可变 id。"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mm.common import (VerifyError, canonical, pack_block, root_hash,
                      sha_bytes, unpack_block)
from mm.manifest import make_manifest, verify_manifest


def _objs(n=5):
    """(oid, type, obj) —— make_manifest 的对象集合格式。"""
    return [(f"obj/{i}", "tile", {"i": i, "v": "x" * (200 + i)})
            for i in range(n)]


def _pack_pairs():
    """(oid, obj) —— pack_block/unpack_block 的格式。"""
    return [(oid, obj) for oid, _t, obj in _objs()]


class BlockFormatTests(unittest.TestCase):
    def test_roundtrip_and_hashes(self):
        blob = pack_block(_pack_pairs())
        want = {oid: sha_bytes(canonical(o)) for oid, o in _pack_pairs()}
        items = unpack_block(blob, expect_block_sha=sha_bytes(blob),
                             expect_objects=want)
        self.assertEqual([i[0] for i in items], [oid for oid, _ in _pack_pairs()])

    def test_corrupt_block_hash_rejected(self):
        blob = bytearray(pack_block(_pack_pairs()))
        blob[20] ^= 0x01
        # gzip 可能整体损坏或对象散列不符，但绝不静默通过
        with self.assertRaises(VerifyError) as c:
            unpack_block(bytes(blob))
        self.assertIn(c.exception.code,
                      {"BLOCK_CORRUPT", "OBJECT_HASH_MISMATCH"})

    def test_expect_block_sha_strict(self):
        blob = pack_block(_pack_pairs())
        with self.assertRaises(VerifyError) as c:
            unpack_block(blob, expect_block_sha="sha256:0" * 8)
        self.assertEqual(c.exception.code, "BLOCK_HASH_MISMATCH")

    def test_object_hash_against_manifest(self):
        blob = pack_block(_pack_pairs())
        expect = {oid: "sha256:deadbeef" for oid, _ in _pack_pairs()}
        with self.assertRaises(VerifyError) as c:
            unpack_block(blob, expect_objects=expect)
        self.assertEqual(c.exception.code, "OBJECT_HASH_MISMATCH")


class ManifestTests(unittest.TestCase):
    def _mf(self, **kw):
        m, blocks = make_manifest(
            snapshot_id="S-1", center_anchor="sha256:abc",
            objects=_objs(), devices=["BOX-A"], block_size=512, **kw)
        return m, blocks

    def test_deterministic_id_and_root(self):
        m1, _ = self._mf()
        m2, _ = self._mf()
        self.assertEqual(m1["manifest_id"], m2["manifest_id"])
        self.assertEqual(m1["root_sha256"], m2["root_sha256"])
        verify_manifest(m1)  # 不抛异常即通过

    def test_block_partition_covers_every_object_once(self):
        m, blocks = self._mf()
        self.assertGreater(len(blocks), 1)
        seen = [o for b in m["blocks"] for o in b["objects"]]
        self.assertEqual(sorted(seen), sorted(e["oid"] for e in m["objects"]))
        self.assertEqual(len(seen), len(set(seen)))

    def test_root_tamper_rejected(self):
        m, _ = self._mf()
        m["root_sha256"] = "sha256:0"
        with self.assertRaises(VerifyError) as c:
            verify_manifest(m)
        self.assertEqual(c.exception.code, "ROOT_HASH_MISMATCH")

    def test_id_tamper_rejected(self):
        m, _ = self._mf()
        m["note"] = "篡改"
        with self.assertRaises(VerifyError) as c:
            verify_manifest(m)
        self.assertEqual(c.exception.code, "MANIFEST_ID_MISMATCH")

    def test_empty_devices_rejected(self):
        with self.assertRaises(ValueError):
            make_manifest(snapshot_id="S-1", center_anchor="sha256:a",
                          objects=_objs(), devices=[])

    def test_root_hash_is_over_entries(self):
        entries = [{"oid": "a", "sha256": "x", "size": 1, "type": "tile"}]
        self.assertTrue(root_hash(entries).startswith("sha256:"))
        self.assertNotEqual(root_hash(entries), root_hash(
            [{**entries[0], "sha256": "y"}]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
