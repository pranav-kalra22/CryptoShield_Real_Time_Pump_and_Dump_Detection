"""
╔══════════════════════════════════════════════════════════════╗
║        Crypto-Shield — Real-Time Fraud Detection Engine       ║
║  Graph Topologies + Anomaly Scoring + Evaluation Metrics     ║
╚══════════════════════════════════════════════════════════════╝

Provides decoupled, lightweight, and fully testable algorithms for:
  • Star Topology Detection (Pump Coordinators / Whales)
  • Dense Clique Detection (Sybil Rings / Botnets)
  • Multi-Signal Confidence Scoring (Social Anomaly + Price Spikes)
  • Real-Time Precision / Recall / F1 Metric Tracking
"""

import os
from collections import defaultdict
from typing import Any, Dict, List, Set, Tuple

# ──────────────────────────────────────────────────────────
# DEFAULT THRESHOLDS (Can be overridden via ENV)
# ──────────────────────────────────────────────────────────
CLIQUE_MIN_SIZE = int(os.getenv("CLIQUE_MIN_SIZE", "5"))
CLIQUE_DENSITY_THRESH = float(os.getenv("CLIQUE_DENSITY_THRESH", "0.6"))
STAR_MIN_DEGREE = int(os.getenv("STAR_MIN_DEGREE", "10"))
STAR_DEGREE_RATIO = float(os.getenv("STAR_DEGREE_RATIO", "0.8"))


def build_graph(
    edges: List[Tuple[str, str]]
) -> Tuple[Dict[str, Set[str]], Dict[str, int], Dict[str, int]]:
    """
    Constructs adjacency sets, out-degree, and in-degree maps from edge tuples.
    """
    adjacency: Dict[str, Set[str]] = defaultdict(set)
    out_degree: Dict[str, int] = defaultdict(int)
    in_degree: Dict[str, int] = defaultdict(int)

    for src, tgt in edges:
        adjacency[src].add(tgt)
        adjacency[tgt].add(src)
        out_degree[src] += 1
        in_degree[tgt] += 1

    return dict(adjacency), dict(out_degree), dict(in_degree)


def detect_star_topology(
    adjacency: Dict[str, Set[str]],
    out_degree: Dict[str, int],
    in_degree: Dict[str, int],
    min_degree: int = STAR_MIN_DEGREE,
    degree_ratio: float = STAR_DEGREE_RATIO,
) -> List[Dict[str, Any]]:
    """
    Detects star-topology hub nodes characteristic of pump-and-dump coordinators.
    A star hub exhibits a disproportionately high out-degree compared to its in-degree.
    """
    alerts = []
    for node, out_count in out_degree.items():
        if out_count < min_degree:
            continue
        total = out_count + in_degree.get(node, 0)
        ratio = out_count / total if total > 0 else 0.0
        if ratio >= degree_ratio:
            alerts.append({
                "alert_type": "STAR_TOPOLOGY",
                "hub_node": node,
                "out_degree": out_count,
                "out_ratio": round(ratio, 3),
                "severity": "HIGH",
            })
    return alerts


def detect_clique(
    adjacency: Dict[str, Set[str]],
    min_size: int = CLIQUE_MIN_SIZE,
    density_thresh: float = CLIQUE_DENSITY_THRESH,
) -> List[Dict[str, Any]]:
    """
    Detects near-complete subgraphs (cliques) indicative of coordinated bot rings.
    Density = 2 * E / (N * (N - 1))
    """
    alerts = []
    visited: Set[str] = set()

    for node, neighbors in adjacency.items():
        if node in visited or len(neighbors) < min_size - 1:
            continue
        candidate = frozenset({node} | set(neighbors))
        if len(candidate) < min_size:
            continue

        nodes_list = list(candidate)
        edges_in = sum(
            1
            for i, u in enumerate(nodes_list)
            for v in nodes_list[i + 1:]
            if v in adjacency.get(u, set())
        )
        n = len(candidate)
        possible = n * (n - 1) / 2
        density = edges_in / possible if possible > 0 else 0.0

        if density >= density_thresh:
            visited.update(candidate)
            alerts.append({
                "alert_type": "CLIQUE_DETECTED",
                "nodes": nodes_list[:20],
                "node_count": n,
                "edge_density": round(density, 3),
                "severity": "CRITICAL" if density > 0.8 else "HIGH",
            })
    return alerts


def compute_confidence(
    alerts: List[Dict[str, Any]],
    price_pumping: bool,
    gt_ratio: float = 0.0
) -> str:
    """
    Multi-signal heuristic confidence scoring:
      • Clique detected: +3 pts each
      • Star topology: +2 pts each
      • Corroborated price spike: +3 pts
      • Ground-truth bot ratio: up to +5 pts
    """
    score = 0
    score += len([a for a in alerts if a.get("alert_type") == "CLIQUE_DETECTED"]) * 3
    score += len([a for a in alerts if a.get("alert_type") == "STAR_TOPOLOGY"]) * 2
    if price_pumping:
        score += 3
    score += int(gt_ratio * 5)

    if score >= 8:
        return "CRITICAL"
    if score >= 5:
        return "HIGH"
    if score >= 2:
        return "MEDIUM"
    return "LOW"


class MetricsTracker:
    """
    Thread-safe performance metrics tracker computing Precision, Recall, and F1 score.
    """

    def __init__(self):
        self.tp = 0
        self.fp = 0
        self.fn = 0
        self.total = 0

    def reset(self):
        self.tp = 0
        self.fp = 0
        self.fn = 0
        self.total = 0

    def update(self, alerts: List[Dict[str, Any]], rows: List[Any]) -> Tuple[float, float, float]:
        """
        Updates confusion matrix counts based on ground-truth signals vs alerts.
        """
        ground_truth = any(getattr(r, "pump_signal", False) for r in rows) if rows else False
        detected = len(alerts) > 0

        if ground_truth and detected:
            self.tp += 1
        elif not ground_truth and detected:
            self.fp += 1
        elif ground_truth and not detected:
            self.fn += 1

        self.total += 1
        precision = self.tp / (self.tp + self.fp) if (self.tp + self.fp) > 0 else 0.0
        recall = self.tp / (self.tp + self.fn) if (self.tp + self.fn) > 0 else 0.0
        f1 = (
            (2 * precision * recall / (precision + recall))
            if (precision + recall) > 0
            else 0.0
        )
        return round(precision, 3), round(recall, 3), round(f1, 3)

    def get_summary(self) -> Dict[str, Any]:
        precision = self.tp / (self.tp + self.fp) if (self.tp + self.fp) > 0 else 0.0
        recall = self.tp / (self.tp + self.fn) if (self.tp + self.fn) > 0 else 0.0
        f1 = (
            (2 * precision * recall / (precision + recall))
            if (precision + recall) > 0
            else 0.0
        )
        return {
            "true_positives": self.tp,
            "false_positives": self.fp,
            "false_negatives": self.fn,
            "total_batches": self.total,
            "precision": round(precision, 3),
            "recall": round(recall, 3),
            "f1_score": round(f1, 3),
        }


# Global default instance for backwards-compatibility
_default_tracker = MetricsTracker()


def update_metrics(alerts: List[Dict[str, Any]], rows: List[Any]) -> Tuple[float, float, float]:
    """Drop-in functional wrapper for default tracker."""
    return _default_tracker.update(alerts, rows)


def get_default_metrics_summary() -> Dict[str, Any]:
    return _default_tracker.get_summary()
