import hashlib
import math


class BloomFilter:
    """Probabilistic set membership test: no false negatives, tunable false-positive rate.

    Used so a lookup can skip reading an SSTable from disk entirely when the
    key is definitely not in it.
    """

    def __init__(self, expected_items: int = 1000, false_positive_rate: float = 0.01):
        self.size = self._optimal_size(expected_items, false_positive_rate)
        self.num_hashes = self._optimal_num_hashes(self.size, expected_items)
        self.bits = bytearray((self.size + 7) // 8)

    @staticmethod
    def _optimal_size(n: int, p: float) -> int:
        n = max(n, 1)
        return max(8, math.ceil(-(n * math.log(p)) / (math.log(2) ** 2)))

    @staticmethod
    def _optimal_num_hashes(m: int, n: int) -> int:
        n = max(n, 1)
        return max(1, round((m / n) * math.log(2)))

    def _positions(self, item: str):
        for i in range(self.num_hashes):
            digest = hashlib.sha256(f"{i}:{item}".encode()).hexdigest()
            yield int(digest, 16) % self.size

    def add(self, item: str):
        for pos in self._positions(item):
            self.bits[pos // 8] |= 1 << (pos % 8)

    def might_contain(self, item: str) -> bool:
        return all(self.bits[pos // 8] & (1 << (pos % 8)) for pos in self._positions(item))

    def to_dict(self) -> dict:
        return {
            "size": self.size,
            "num_hashes": self.num_hashes,
            "bits": self.bits.hex(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BloomFilter":
        instance = cls.__new__(cls)
        instance.size = data["size"]
        instance.num_hashes = data["num_hashes"]
        instance.bits = bytearray.fromhex(data["bits"])
        return instance
