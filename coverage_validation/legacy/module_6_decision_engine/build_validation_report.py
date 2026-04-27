"""
Build Validation Report

CLI wrapper for decision engine that loads inputs and generates validation report.
"""

import json
from pathlib import Path
from typing import Dict, Any

from decision_engine import create_decision_engine


class ValidationReportBuilder:
    """Builds validation report from comparison results."""
    
    def __init__(self):
        """Initialize builder."""
        self.engine = create_decision_engine()
    
    def build_report(
        self,
        comparison_results_file: str,
        canonical_views_file: str,
        cru_units_file: str
    ) -> Dict[str, Any]:
        """
        Build validation report.
        
        Args:
            comparison_results_file: Path to comparison_results.json
            canonical_views_file: Path to canonical_acceptance_views.json
            cru_units_file: Path to cru_units.json
            
        Returns:
            Validation report dictionary
        """
        print(f"\n{'='*70}")
        print("BUILD VALIDATION REPORT")
        print(f"{'='*70}\n")
        
        # Load comparison results
        print("Loading comparison results...")
        with open(comparison_results_file, 'r', encoding='utf-8') as f:
            comparison_data = json.load(f)
        comparisons = comparison_data["comparisons"]
        print(f"Loaded {len(comparisons)} comparisons\n")
        
        # Load canonical views
        print("Loading canonical acceptance views...")
        with open(canonical_views_file, 'r', encoding='utf-8') as f:
            views_data = json.load(f)
        canonical_views = views_data["canonical_views"]
        print(f"Loaded {len(canonical_views)} canonical views\n")
        
        # Load CRU units
        print("Loading CRU units...")
        with open(cru_units_file, 'r', encoding='utf-8') as f:
            cru_data = json.load(f)
        all_crus = cru_data["crus"]
        print(f"Loaded {len(all_crus)} CRU units\n")
        
        # Generate report
        print(f"{'='*70}")
        print("GENERATING VALIDATION REPORT")
        print(f"{'='*70}\n")
        
        report = self.engine.generate_report(
            comparisons=comparisons,
            canonical_views=canonical_views,
            all_crus=all_crus
        )
        
        # Convert to dictionary
        report_dict = {
            "metadata": report.metadata,
            "scenario_analysis": report.scenario_analysis,
            "requirement_analysis": report.requirement_analysis,
            "gaps": report.gaps
        }
        
        # Print summary
        self._print_summary(report_dict)
        
        return report_dict
    
    def _print_summary(self, report: Dict[str, Any]):
        """Print validation report summary."""
        metadata = report["metadata"]
        gaps = report["gaps"]
        
        print(f"{'='*70}")
        print("VALIDATION REPORT SUMMARY")
        print(f"{'='*70}\n")
        
        print("GLOBAL METRICS:")
        print(f"  Total scenarios: {metadata['total_scenarios']}")
        print(f"  Overall coverage score: {metadata['overall_coverage_score']:.4f}")
        print(f"  Total requirements: {metadata['total_requirements']}")
        print()
        
        print("RISK DISTRIBUTION:")
        risk_dist = metadata['risk_distribution']
        total = metadata['total_scenarios']
        for risk_level in ["LOW_RISK", "MEDIUM_RISK", "HIGH_RISK"]:
            count = risk_dist[risk_level]
            pct = (count / total * 100) if total > 0 else 0.0
            print(f"  {risk_level}: {count} ({pct:.1f}%)")
        print()
        
        print("REQUIREMENT VALIDATION:")
        print(f"  Validated: {metadata['validated_requirements']}")
        print(f"  Weakly validated: {metadata['weakly_validated_requirements']}")
        print(f"  Unused: {metadata['unused_requirements']} ({metadata['unused_percentage']:.1f}%)")
        print()
        
        print("GAPS IDENTIFIED:")
        print(f"  High-risk scenarios: {len(gaps['high_risk_scenarios'])}")
        if gaps['high_risk_scenarios']:
            print("    Scenario IDs:", ", ".join(s['scenario_id'] for s in gaps['high_risk_scenarios']))
        
        print(f"  Unused requirements: {len(gaps['unused_requirements'])}")
        if gaps['unused_requirements']:
            print("    CRU IDs:", ", ".join(r['cru_id'] for r in gaps['unused_requirements']))
        
        print(f"  Weakly validated requirements: {len(gaps['weakly_validated_requirements'])}")
        if gaps['weakly_validated_requirements']:
            print("    CRU IDs:", ", ".join(r['cru_id'] for r in gaps['weakly_validated_requirements']))
        
        print(f"\n{'='*70}\n")


def build_validation_report(
    comparison_results_file: str,
    canonical_views_file: str,
    cru_units_file: str,
    output_file: str = "validation_report.json"
):
    """
    Build validation report and save to JSON.
    
    Args:
        comparison_results_file: Path to comparison results
        canonical_views_file: Path to canonical views
        cru_units_file: Path to CRU units
        output_file: Output JSON file path
    """
    builder = ValidationReportBuilder()
    
    report = builder.build_report(
        comparison_results_file=comparison_results_file,
        canonical_views_file=canonical_views_file,
        cru_units_file=cru_units_file
    )
    
    # Save report
    print(f"Saving validation report to {output_file}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Validation report saved successfully\n")
    
    return report


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python build_validation_report.py <comparison_results_file> <canonical_views_file> <cru_units_file> [output_file]")
        sys.exit(1)
    
    comparison_results_file = sys.argv[1]
    canonical_views_file = sys.argv[2]
    cru_units_file = sys.argv[3]
    output_file = sys.argv[4] if len(sys.argv) > 4 else "validation_report.json"
    
    build_validation_report(
        comparison_results_file=comparison_results_file,
        canonical_views_file=canonical_views_file,
        cru_units_file=cru_units_file,
        output_file=output_file
    )
