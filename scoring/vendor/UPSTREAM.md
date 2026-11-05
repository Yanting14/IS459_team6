# IBM Multi-GNN source

- Repository: https://github.com/IBM/Multi-GNN
- Commit: `252b0252afca109d1d216c411c59ff70753b25fc`
- Files: `models.py` and `LICENSE`, copied without modification from the clean local checkout.
- License: Apache License 2.0; see LICENSE in this directory.

The complete IBM model module is retained without modification. This repository's CLI is `python -m scoring.pipeline`. `feature_engineering.py` and `graph_sampling.py` were copied byte-for-byte from the supplied Big Data project's `model_training` package; they are project feature/sampling code, not IBM upstream files. No datasets or trained weights are committed here.

When updating this copy, record the new source commit and verify checkpoint compatibility. Existing checkpoints record a SHA256 of models.py, and prediction rejects a different model source.
