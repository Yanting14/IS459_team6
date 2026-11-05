"""Directed incoming-neighbour sampling over chronological transaction IDs."""
import numpy as np


class IncomingSampler:
    """Sample a bounded graph while keeping every target transaction present."""

    def __init__(self, edges, incoming, pointers, neighbours=(10, 5)):
        self.edges = edges
        self.incoming = incoming
        self.pointers = pointers
        self.neighbours = tuple(neighbours)

    def sample(self, seeds, context_end, rng):
        seeds = np.asarray(seeds, dtype=np.int64)
        if len(seeds) == 0 or seeds.min() < 0 or seeds.max() >= context_end:
            raise ValueError('Seeds must belong to the visible context')
        selected = [seeds]
        frontier = np.unique(self.edges[seeds].reshape(-1))
        for fanout in self.neighbours:
            lo = self.pointers[frontier].copy()
            hi = self.pointers[frontier + 1].copy()
            starts = lo.copy()
            # Each node's incoming list is sorted by chronological edge position.
            while np.any(lo < hi):
                active = lo < hi
                mid = (lo + hi) // 2
                values = np.full(len(mid), context_end, dtype=np.int64)
                values[active] = self.incoming[mid[active]]
                left = active & (values < context_end)
                right = active & ~left
                lo[left] = mid[left] + 1
                hi[right] = mid[right]
            lengths = lo - starts
            picks = np.full((len(frontier), fanout), -1, dtype=np.int64)
            for k in range(fanout):
                live = lengths > k
                candidate = (rng.random(len(frontier)) * np.maximum(lengths, 1)).astype(np.int64)
                if k:
                    collision = live & (picks[:, :k] == candidate[:, None]).any(1)
                    while collision.any():
                        candidate[collision] = (rng.random(int(collision.sum())) * lengths[collision]).astype(np.int64)
                        collision = live & (picks[:, :k] == candidate[:, None]).any(1)
                picks[live, k] = candidate[live]
            valid = picks >= 0
            sampled = self.incoming[(starts[:, None] + np.maximum(picks, 0))[valid]]
            selected.append(sampled)
            frontier = np.unique(self.edges[sampled, 0])
        edge_ids = np.unique(np.concatenate(selected))
        nodes, local = np.unique(self.edges[edge_ids], return_inverse=True)
        positions = np.searchsorted(edge_ids, seeds)
        if edge_ids.max() >= context_end:
            raise AssertionError('Future edge entered the sampled graph')
        np.testing.assert_array_equal(edge_ids[positions], seeds)
        return nodes, edge_ids, local.reshape(-1, 2).T, positions
