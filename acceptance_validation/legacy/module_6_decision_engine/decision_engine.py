"""
Decision Engine

Pure aggregation logic for computing coverage metrics and validation scores.
No LLM, no embeddings, no reclassification - only deterministic aggregation.
"""

from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from collections import defaultdict


@dataclass
class ScenarioMetrics:
    """Metrics for a single scenario."""
    scenario_id: str
    total_steps: int
    matches: int
    partial_matches: int
    missing: int
    conflicts: int
    coverage_score: float
    risk_level: str


@dataclass
class RequirementMetrics:
    """Metrics for a single CRU requirement."""
    cru_id: str
    usage_count: int
    match_count: int
    partial_count: int
    missing_count: int
    conflict_count: int
    status: str


@dataclass
class ValidationReport:
    """Complete validation report."""
    metadata: Dict[str, Any]
    scenario_analysis: List[Dict[str, Any]]
    requirement_analysis: List[Dict[str, Any]]
    gaps: Dict[str, Any]


class DecisionEngine:
    """Deterministic decision engine for validation analysis."""
    
    def __init__(self):
        """Initialize decision engine."""
        self.scenario_metrics: Dict[str, ScenarioMetrics] = {}
        self.requirement_metrics: Dict[str, RequirementMetrics] = {}
    
    def _compute_coverage_score(
        self,
        matches: int,
        partial_matches: int,
        missing: int,
        conflicts: int,
        total_steps: int
    ) -> float:
        """
        Compute coverage score for a scenario.
        
        Formula:
        score = (matches * 1.0 + partial * 0.5 + missing * 0.0 + conflicts * -1.0) / total
        
        Clamped to [0, 1]
        """
        if total_steps == 0:
            return 0.0
        
        raw_score = (
            matches * 1.0 +
            partial_matches * 0.5 +
            missing * 0.0 +
            conflicts * -1.0
        ) / total_steps
        
        return max(0.0, min(1.0, raw_score))
    
    def _classify_risk_level(self, coverage_score: float) -> str:
        """
        Classify scenario risk level based on coverage score.
        
        Args:
            coverage_score: Coverage score [0, 1]
            
        Returns:
            Risk level: LOW_RISK, MEDIUM_RISK, or HIGH_RISK
        """
        if coverage_score >= 0.75:
            return "LOW_RISK"
        elif coverage_score >= 0.5:
            return "MEDIUM_RISK"
        else:
            return "HIGH_RISK"
    
    def _classify_requirement_status(
        self,
        usage_count: int,
        match_count: int,
        partial_count: int
    ) -> str:
        """
        Classify requirement validation status.
        
        Args:
            usage_count: Total times referenced
            match_count: Times matched
            partial_count: Times partially matched
            
        Returns:
            Status: VALIDATED, WEAKLY_VALIDATED, or UNUSED_REQUIREMENT
        """
        if usage_count == 0:
            return "UNUSED_REQUIREMENT"
        elif match_count > 0:
            return "VALIDATED"
        elif partial_count > 0:
            return "WEAKLY_VALIDATED"
        else:
            return "UNUSED_REQUIREMENT"
    
    def analyze_scenarios(
        self,
        comparisons: List[Dict[str, Any]],
        canonical_views: List[Dict[str, Any]]
    ) -> List[ScenarioMetrics]:
        """
        Analyze scenarios and compute coverage metrics.
        
        Args:
            comparisons: List of comparison results
            canonical_views: List of canonical acceptance views
            
        Returns:
            List of ScenarioMetrics
        """
        # Group comparisons by scenario
        scenario_groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        for comp in comparisons:
            acceptance_id = comp["acceptance_id"]
            # Extract scenario ID (remove _step_N suffix)
            if "_step_" in acceptance_id:
                scenario_id = acceptance_id.split("_step_")[0]
            else:
                scenario_id = acceptance_id
            
            scenario_groups[scenario_id].append(comp)
        
        # Compute metrics for each scenario
        scenario_metrics_list = []
        
        for scenario_id, group in scenario_groups.items():
            # Count classifications
            matches = sum(1 for c in group if c["classification"] == "MATCH")
            partial = sum(1 for c in group if c["classification"] == "PARTIAL_MATCH")
            missing = sum(1 for c in group if c["classification"] == "MISSING_REQUIREMENT")
            conflicts = sum(1 for c in group if c["classification"] == "CONFLICT")
            total = len(group)
            
            # Compute coverage score
            coverage_score = self._compute_coverage_score(
                matches=matches,
                partial_matches=partial,
                missing=missing,
                conflicts=conflicts,
                total_steps=total
            )
            
            # Classify risk
            risk_level = self._classify_risk_level(coverage_score)
            
            # Create metrics
            metrics = ScenarioMetrics(
                scenario_id=scenario_id,
                total_steps=total,
                matches=matches,
                partial_matches=partial,
                missing=missing,
                conflicts=conflicts,
                coverage_score=round(coverage_score, 4),
                risk_level=risk_level
            )
            
            scenario_metrics_list.append(metrics)
            self.scenario_metrics[scenario_id] = metrics
        
        return scenario_metrics_list
    
    def analyze_requirements(
        self,
        comparisons: List[Dict[str, Any]],
        all_crus: List[Dict[str, Any]]
    ) -> List[RequirementMetrics]:
        """
        Analyze CRU requirements and compute usage metrics.
        
        Args:
            comparisons: List of comparison results
            all_crus: List of all CRU units
            
        Returns:
            List of RequirementMetrics
        """
        # Initialize counters for all CRUs
        cru_counters: Dict[str, Dict[str, int]] = {}
        for cru in all_crus:
            cru_id = cru["cru_id"]
            cru_counters[cru_id] = {
                "usage": 0,
                "match": 0,
                "partial": 0,
                "missing": 0,
                "conflict": 0
            }
        
        # Count references
        for comp in comparisons:
            best_cru = comp.get("best_matching_cru")
            classification = comp["classification"]
            
            if best_cru and best_cru in cru_counters:
                cru_counters[best_cru]["usage"] += 1
                
                if classification == "MATCH":
                    cru_counters[best_cru]["match"] += 1
                elif classification == "PARTIAL_MATCH":
                    cru_counters[best_cru]["partial"] += 1
                elif classification == "MISSING_REQUIREMENT":
                    cru_counters[best_cru]["missing"] += 1
                elif classification == "CONFLICT":
                    cru_counters[best_cru]["conflict"] += 1
        
        # Build requirement metrics
        requirement_metrics_list = []
        
        for cru_id, counts in cru_counters.items():
            status = self._classify_requirement_status(
                usage_count=counts["usage"],
                match_count=counts["match"],
                partial_count=counts["partial"]
            )
            
            metrics = RequirementMetrics(
                cru_id=cru_id,
                usage_count=counts["usage"],
                match_count=counts["match"],
                partial_count=counts["partial"],
                missing_count=counts["missing"],
                conflict_count=counts["conflict"],
                status=status
            )
            
            requirement_metrics_list.append(metrics)
            self.requirement_metrics[cru_id] = metrics
        
        return requirement_metrics_list
    
    def identify_gaps(
        self,
        scenario_metrics: List[ScenarioMetrics],
        requirement_metrics: List[RequirementMetrics]
    ) -> Dict[str, Any]:
        """
        Identify validation gaps.
        
        Args:
            scenario_metrics: Scenario analysis results
            requirement_metrics: Requirement analysis results
            
        Returns:
            Gap analysis dictionary
        """
        # High-risk scenarios
        high_risk_scenarios = [
            {
                "scenario_id": m.scenario_id,
                "coverage_score": m.coverage_score,
                "risk_level": m.risk_level
            }
            for m in scenario_metrics
            if m.risk_level == "HIGH_RISK"
        ]
        
        # Unused requirements
        unused_requirements = [
            {
                "cru_id": m.cru_id,
                "status": m.status
            }
            for m in requirement_metrics
            if m.status == "UNUSED_REQUIREMENT"
        ]
        
        # Weakly validated requirements
        weakly_validated_requirements = [
            {
                "cru_id": m.cru_id,
                "usage_count": m.usage_count,
                "partial_count": m.partial_count,
                "status": m.status
            }
            for m in requirement_metrics
            if m.status == "WEAKLY_VALIDATED"
        ]
        
        return {
            "high_risk_scenarios": high_risk_scenarios,
            "unused_requirements": unused_requirements,
            "weakly_validated_requirements": weakly_validated_requirements
        }
    
    def compute_global_metrics(
        self,
        scenario_metrics: List[ScenarioMetrics],
        requirement_metrics: List[RequirementMetrics]
    ) -> Dict[str, Any]:
        """
        Compute global validation metrics.
        
        Args:
            scenario_metrics: Scenario analysis results
            requirement_metrics: Requirement analysis results
            
        Returns:
            Global metrics dictionary
        """
        total_scenarios = len(scenario_metrics)
        
        # Risk distribution
        risk_counts = {"LOW_RISK": 0, "MEDIUM_RISK": 0, "HIGH_RISK": 0}
        for m in scenario_metrics:
            risk_counts[m.risk_level] += 1
        
        # Overall coverage score
        if total_scenarios > 0:
            overall_coverage = sum(m.coverage_score for m in scenario_metrics) / total_scenarios
        else:
            overall_coverage = 0.0
        
        # Requirement statistics
        total_requirements = len(requirement_metrics)
        unused_count = sum(1 for m in requirement_metrics if m.status == "UNUSED_REQUIREMENT")
        validated_count = sum(1 for m in requirement_metrics if m.status == "VALIDATED")
        weakly_validated_count = sum(1 for m in requirement_metrics if m.status == "WEAKLY_VALIDATED")
        
        return {
            "total_scenarios": total_scenarios,
            "overall_coverage_score": round(overall_coverage, 4),
            "risk_distribution": risk_counts,
            "total_requirements": total_requirements,
            "validated_requirements": validated_count,
            "weakly_validated_requirements": weakly_validated_count,
            "unused_requirements": unused_count,
            "unused_percentage": round(unused_count / total_requirements * 100, 2) if total_requirements > 0 else 0.0
        }
    
    def generate_report(
        self,
        comparisons: List[Dict[str, Any]],
        canonical_views: List[Dict[str, Any]],
        all_crus: List[Dict[str, Any]]
    ) -> ValidationReport:
        """
        Generate complete validation report.
        
        Args:
            comparisons: Comparison results
            canonical_views: Canonical acceptance views
            all_crus: All CRU units
            
        Returns:
            ValidationReport
        """
        # Analyze scenarios
        scenario_metrics = self.analyze_scenarios(comparisons, canonical_views)
        
        # Analyze requirements
        requirement_metrics = self.analyze_requirements(comparisons, all_crus)
        
        # Identify gaps
        gaps = self.identify_gaps(scenario_metrics, requirement_metrics)
        
        # Compute global metrics
        global_metrics = self.compute_global_metrics(scenario_metrics, requirement_metrics)
        
        # Build report
        return ValidationReport(
            metadata=global_metrics,
            scenario_analysis=[
                {
                    "scenario_id": m.scenario_id,
                    "coverage_score": m.coverage_score,
                    "risk_level": m.risk_level,
                    "total_steps": m.total_steps,
                    "matches": m.matches,
                    "partial_matches": m.partial_matches,
                    "missing": m.missing,
                    "conflicts": m.conflicts
                }
                for m in sorted(scenario_metrics, key=lambda x: x.scenario_id)
            ],
            requirement_analysis=[
                {
                    "cru_id": m.cru_id,
                    "usage_count": m.usage_count,
                    "match_count": m.match_count,
                    "partial_count": m.partial_count,
                    "missing_count": m.missing_count,
                    "conflict_count": m.conflict_count,
                    "status": m.status
                }
                for m in sorted(requirement_metrics, key=lambda x: x.cru_id)
            ],
            gaps=gaps
        )


def create_decision_engine() -> DecisionEngine:
    """Factory function to create decision engine."""
    return DecisionEngine()
