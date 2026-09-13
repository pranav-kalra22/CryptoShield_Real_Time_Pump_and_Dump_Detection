"""
Unit Tests for CryptoShield Fraud Detection Engine
Tests graph topology algorithms, confidence scoring, and evaluation metrics.
"""

import unittest
from types import SimpleNamespace
from detection_engine import (
    build_graph,
    detect_star_topology,
    detect_clique,
    compute_confidence,
    MetricsTracker,
)


class TestDetectionEngine(unittest.TestCase):

    def test_build_graph(self):
        edges = [("alice", "bob"), ("alice", "charlie"), ("bob", "alice")]
        adj, out_deg, in_deg = build_graph(edges)

        self.assertEqual(out_deg["alice"], 2)
        self.assertEqual(in_deg["alice"], 1)
        self.assertEqual(out_deg["bob"], 1)
        self.assertEqual(in_deg["bob"], 1)
        self.assertEqual(out_deg.get("charlie", 0), 0)
        self.assertEqual(in_deg["charlie"], 1)

        self.assertIn("bob", adj["alice"])
        self.assertIn("charlie", adj["alice"])

    def test_star_topology_positive(self):
        # Coordinator whale sends instructions to 15 bot accounts
        edges = [(f"coordinator_whale", f"puppet_{i}") for i in range(15)]
        adj, out_deg, in_deg = build_graph(edges)

        alerts = detect_star_topology(adj, out_deg, in_deg, min_degree=10, degree_ratio=0.8)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["alert_type"], "STAR_TOPOLOGY")
        self.assertEqual(alerts[0]["hub_node"], "coordinator_whale")
        self.assertEqual(alerts[0]["out_degree"], 15)
        self.assertEqual(alerts[0]["out_ratio"], 1.0)
        self.assertEqual(alerts[0]["severity"], "HIGH")

    def test_star_topology_negative_low_degree(self):
        # Out-degree is only 5 (below threshold of 10)
        edges = [(f"coordinator_small", f"puppet_{i}") for i in range(5)]
        adj, out_deg, in_deg = build_graph(edges)

        alerts = detect_star_topology(adj, out_deg, in_deg, min_degree=10, degree_ratio=0.8)
        self.assertEqual(len(alerts), 0)

    def test_star_topology_negative_balanced_degree(self):
        # Out-degree 12, In-degree 12 -> ratio = 0.5 (below 0.8)
        edges = [("user_popular", f"friend_{i}") for i in range(12)]
        edges += [(f"friend_{i}", "user_popular") for i in range(12)]
        adj, out_deg, in_deg = build_graph(edges)

        alerts = detect_star_topology(adj, out_deg, in_deg, min_degree=10, degree_ratio=0.8)
        self.assertEqual(len(alerts), 0)

    def test_clique_detection_positive(self):
        # 6 bots all interconnected (K_6 complete graph -> density 1.0)
        bots = [f"bot_{i}" for i in range(6)]
        edges = []
        for i in range(len(bots)):
            for j in range(i + 1, len(bots)):
                edges.append((bots[i], bots[j]))
        adj, _, _ = build_graph(edges)

        alerts = detect_clique(adj, min_size=5, density_thresh=0.6)
        self.assertGreaterEqual(len(alerts), 1)
        clique = alerts[0]
        self.assertEqual(clique["alert_type"], "CLIQUE_DETECTED")
        self.assertEqual(clique["node_count"], 6)
        self.assertEqual(clique["edge_density"], 1.0)
        self.assertEqual(clique["severity"], "CRITICAL")

    def test_clique_detection_negative_sparse(self):
        # Path / chain graph: 0-1-2-3-4-5-6-7 (very low density)
        edges = [(f"user_{i}", f"user_{i+1}") for i in range(8)]
        adj, _, _ = build_graph(edges)

        alerts = detect_clique(adj, min_size=5, density_thresh=0.6)
        self.assertEqual(len(alerts), 0)

    def test_compute_confidence_scoring(self):
        # Low score
        self.assertEqual(compute_confidence([], price_pumping=False, gt_ratio=0.0), "LOW")

        # Medium score (1 star alert = 2 pts)
        star_alert = [{"alert_type": "STAR_TOPOLOGY"}]
        self.assertEqual(compute_confidence(star_alert, price_pumping=False, gt_ratio=0.0), "MEDIUM")

        # High score (1 star + price pump = 5 pts)
        self.assertEqual(compute_confidence(star_alert, price_pumping=True, gt_ratio=0.0), "HIGH")

        # Critical score (1 clique + 1 star + price pump = 3 + 2 + 3 = 8 pts)
        combo_alerts = [{"alert_type": "CLIQUE_DETECTED"}, {"alert_type": "STAR_TOPOLOGY"}]
        self.assertEqual(compute_confidence(combo_alerts, price_pumping=True, gt_ratio=0.2), "CRITICAL")

    def test_metrics_tracker(self):
        tracker = MetricsTracker()

        # Batch 1: Ground truth present, alert present -> TP
        row_tp = [SimpleNamespace(pump_signal=True)]
        alerts = [{"alert_type": "STAR_TOPOLOGY"}]
        p, r, f1 = tracker.update(alerts, row_tp)
        self.assertEqual(p, 1.0)
        self.assertEqual(r, 1.0)
        self.assertEqual(f1, 1.0)

        # Batch 2: No ground truth, alert present -> FP
        row_fp = [SimpleNamespace(pump_signal=False)]
        p, r, f1 = tracker.update(alerts, row_fp)
        # TP=1, FP=1, FN=0 -> Precision = 1 / 2 = 0.5, Recall = 1 / 1 = 1.0
        self.assertEqual(p, 0.5)
        self.assertEqual(r, 1.0)

        # Batch 3: Ground truth present, no alert -> FN
        row_fn = [SimpleNamespace(pump_signal=True)]
        p, r, f1 = tracker.update([], row_fn)
        # TP=1, FP=1, FN=1 -> Precision = 0.5, Recall = 0.5, F1 = 0.5
        self.assertEqual(p, 0.5)
        self.assertEqual(r, 0.5)
        self.assertEqual(f1, 0.5)

        summary = tracker.get_summary()
        self.assertEqual(summary["total_batches"], 3)
        self.assertEqual(summary["true_positives"], 1)
        self.assertEqual(summary["false_positives"], 1)
        self.assertEqual(summary["false_negatives"], 1)


if __name__ == "__main__":
    unittest.main()
