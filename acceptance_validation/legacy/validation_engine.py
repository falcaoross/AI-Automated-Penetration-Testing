import json
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Tuple
import re
from difflib import SequenceMatcher
from datetime import datetime


# ============================================
# VALIDATOR 1: LOGICAL CONSISTENCY CHECKER
# ============================================

class LogicalConsistencyValidator:
    """Validates logical consistency of test cases"""
    
    def __init__(self):
        self.forbidden_phrases = [
            'execute test action', 'input test data', 'verify result',
            'test_input', 'works as expected', 'behaves as expected'
        ]
    
    def validate(self, test_case: Dict, requirement: Dict) -> Dict:
        """
        Check logical consistency:
        1. Steps are specific and actionable
        2. Expected result is measurable
        3. Test data is concrete
        4. No placeholder phrases
        """
        
        score = 1.0
        issues = []
        
        # 1. Check for placeholder phrases
        steps_str = ' '.join([str(s).lower() for s in test_case.get('test_steps', [])])
        expected = test_case.get('expected_result', '').lower()
        
        for phrase in self.forbidden_phrases:
            if phrase in steps_str or phrase in expected:
                score -= 0.3
                issues.append(f"Contains placeholder phrase: '{phrase}'")
                break
        
        # 2. Check test steps count (min 4 steps)
        steps = test_case.get('test_steps', [])
        if len(steps) < 4:
            score -= 0.2
            issues.append(f"Insufficient steps: {len(steps)} (min: 4)")
        
        # 3. Check expected result length (min 30 chars)
        if len(expected) < 30:
            score -= 0.2
            issues.append(f"Expected result too short: {len(expected)} chars (min: 30)")
        
        # 4. Check test data quality
        test_data = test_case.get('test_data', {})
        if isinstance(test_data, dict):
            if not test_data:
                score -= 0.1
                issues.append("Empty test data")
            elif any('test_input' in str(v).lower() for v in test_data.values()):
                score -= 0.2
                issues.append("Generic test data (test_input)")
        
        # 5. Check test type appropriateness
        test_type = test_case.get('test_type', '')
        if test_type in ['positive', 'negative']:
            if 'invalid' not in steps_str and test_type == 'negative':
                score -= 0.1
                issues.append("Negative test without 'invalid' scenario")
        
        score = max(0.0, min(1.0, score))
        
        return {
            'score': score,
            'issues': issues,
            'validator': 'logical_consistency'
        }


# ============================================
# VALIDATOR 2: DUPLICATE/UNIQUENESS DETECTOR
# ============================================

class DuplicateDetector:
    """Detects duplicate or highly similar test cases"""
    
    def __init__(self):
        self.similarity_threshold = 0.85  # 85% similarity = duplicate
    
    def check_uniqueness(self, test_case: Dict, all_test_cases: List[Dict]) -> Dict:
        """
        Check if test case is unique by comparing:
        1. Title similarity
        2. Steps similarity
        3. Expected result similarity
        """
        
        current_id = test_case.get('test_id', '')
        current_title = test_case.get('test_title', '').lower()
        current_steps = ' '.join([str(s).lower() for s in test_case.get('test_steps', [])])
        current_expected = test_case.get('expected_result', '').lower()
        
        max_similarity = 0.0
        most_similar_tc = None
        
        for other_tc in all_test_cases:
            # Skip self-comparison
            if other_tc.get('test_id') == current_id:
                continue
            
            # Only compare within same requirement (or related requirements)
            if other_tc.get('requirement_id') != test_case.get('requirement_id'):
                continue
            
            # Calculate similarity scores
            title_sim = self._text_similarity(current_title, other_tc.get('test_title', '').lower())
            steps_sim = self._text_similarity(
                current_steps,
                ' '.join([str(s).lower() for s in other_tc.get('test_steps', [])])
            )
            expected_sim = self._text_similarity(
                current_expected,
                other_tc.get('expected_result', '').lower()
            )
            
            # Weighted average similarity
            overall_sim = (title_sim * 0.3 + steps_sim * 0.5 + expected_sim * 0.2)
            
            if overall_sim > max_similarity:
                max_similarity = overall_sim
                most_similar_tc = other_tc.get('test_id')
        
        # Calculate uniqueness score (inverse of similarity)
        uniqueness_score = 1.0 - max_similarity
        
        issues = []
        if max_similarity > self.similarity_threshold:
            issues.append(f"Highly similar to {most_similar_tc} ({max_similarity:.2%} similarity)")
        
        return {
            'score': uniqueness_score,
            'max_similarity': max_similarity,
            'similar_to': most_similar_tc,
            'issues': issues,
            'validator': 'uniqueness'
        }
    
    def _text_similarity(self, text1: str, text2: str) -> float:
        """Calculate text similarity using SequenceMatcher"""
        return SequenceMatcher(None, text1, text2).ratio()


# ============================================
# VALIDATOR 3: COVERAGE CONTRIBUTION ANALYZER
# ============================================

class CoverageContributionAnalyzer:
    """Analyzes how much a test case contributes to overall coverage"""
    
    def __init__(self):
        self.critical_test_types = ['positive', 'negative', 'edge']
    
    def assess_coverage(self, test_case: Dict, requirement: Dict, existing_tests: List[Dict]) -> Dict:
        """
        Assess coverage contribution:
        1. Does it cover a critical test type?
        2. Does it test a unique scenario?
        3. Does it cover requirement-specific aspects?
        """
        
        score = 0.5  # Base score
        contributions = []
        
        test_type = test_case.get('test_type', '')
        req_id = test_case.get('requirement_id', '')
        
        # Get existing tests for same requirement
        req_tests = [tc for tc in existing_tests if tc.get('requirement_id') == req_id]
        existing_types = set(tc.get('test_type') for tc in req_tests if tc.get('test_id') != test_case.get('test_id'))
        
        # 1. Critical test type bonus
        if test_type in self.critical_test_types:
            score += 0.2
            contributions.append(f"Covers critical type: {test_type}")
            
            # Extra bonus if this type is missing
            if test_type not in existing_types:
                score += 0.2
                contributions.append(f"First {test_type} test for requirement")
        
        # 2. Diversity bonus (covers non-standard types)
        if test_type not in self.critical_test_types:
            if test_type not in existing_types:
                score += 0.15
                contributions.append(f"Adds diversity: {test_type}")
        
        # 3. SRS-specific validation
        req_description = requirement.get('description', '').lower()
        test_title = test_case.get('test_title', '').lower()
        test_steps = ' '.join([str(s).lower() for s in test_case.get('test_steps', [])])
        
        # Check if test addresses specific SRS keywords
        srs_keywords = self._extract_srs_keywords(req_description)
        covered_keywords = [kw for kw in srs_keywords if kw in test_title or kw in test_steps]
        
        if covered_keywords:
            keyword_coverage = len(covered_keywords) / max(len(srs_keywords), 1)
            score += 0.15 * keyword_coverage
            contributions.append(f"Covers SRS keywords: {', '.join(covered_keywords[:3])}")
        
        score = min(1.0, score)
        
        return {
            'score': score,
            'contributions': contributions,
            'validator': 'coverage_contribution'
        }
    
    def _extract_srs_keywords(self, description: str) -> List[str]:
        """Extract important keywords from requirement description"""
        # Common SRS-specific keywords
        important_patterns = [
            'maximum', 'minimum', 'sorted', 'displayed', 'mandatory', 'optional',
            'within', 'result', 'search', 'filter', 'default', 'required'
        ]
        
        keywords = []
        for pattern in important_patterns:
            if pattern in description.lower():
                keywords.append(pattern)
        
        return keywords


# ============================================
# VALIDATOR 4: CONFIDENCE SCORING ENGINE
# ============================================

class ConfidenceScorer:
    """
    Calculates overall confidence score using weighted combination of validators
    
    Research-backed weights:
    - Logical Consistency: 40% (most critical for correctness)
    - Uniqueness: 20% (prevent redundancy)
    - Coverage Contribution: 30% (ensure comprehensive testing)
    - Placeholder Detection: 10% (basic quality gate)
    """
    
    def __init__(self):
        self.weights = {
            'logical_consistency': 0.40,
            'uniqueness': 0.20,
            'coverage_contribution': 0.30,
            'placeholder_check': 0.10
        }
    
    def calculate_confidence(self, validation_results: Dict) -> Dict:
        """Calculate weighted confidence score"""
        
        # Extract scores
        logical_score = validation_results.get('logical_consistency', {}).get('score', 0.0)
        uniqueness_score = validation_results.get('uniqueness', {}).get('score', 0.0)
        coverage_score = validation_results.get('coverage_contribution', {}).get('score', 0.0)
        
        # Placeholder check (inverse of logical consistency issues)
        placeholder_score = 1.0 if not any('placeholder' in issue.lower() 
                                          for issue in validation_results.get('logical_consistency', {}).get('issues', [])) else 0.0
        
        # Weighted calculation
        overall_score = (
            logical_score * self.weights['logical_consistency'] +
            uniqueness_score * self.weights['uniqueness'] +
            coverage_score * self.weights['coverage_contribution'] +
            placeholder_score * self.weights['placeholder_check']
        )
        
        # Determine confidence level
        if overall_score >= 0.8:
            confidence_level = 'HIGH'
            recommendation = 'Production-ready'
        elif overall_score >= 0.6:
            confidence_level = 'MEDIUM'
            recommendation = 'Minor improvements needed'
        elif overall_score >= 0.4:
            confidence_level = 'LOW'
            recommendation = 'Significant improvements required'
        else:
            confidence_level = 'REVIEW_REQUIRED'
            recommendation = 'Manual review and major revisions needed'
        
        return {
            'overall_score': overall_score,
            'confidence_level': confidence_level,
            'recommendation': recommendation,
            'component_scores': {
                'logical_consistency': logical_score,
                'uniqueness': uniqueness_score,
                'coverage_contribution': coverage_score,
                'placeholder_check': placeholder_score
            }
        }


# ============================================
# MAIN VALIDATION ENGINE
# ============================================

class TestCaseValidationEngine:
    """Multi-layer validation engine for test cases"""
    
    def __init__(self):
        self.logical_validator = LogicalConsistencyValidator()
        self.duplicate_detector = DuplicateDetector()
        self.coverage_analyzer = CoverageContributionAnalyzer()
        self.confidence_scorer = ConfidenceScorer()
    
    def validate_single(self, test_case: Dict, requirement: Dict, all_test_cases: List[Dict]) -> Dict:
        """Validate a single test case"""
        
        # Run all validators
        logical_result = self.logical_validator.validate(test_case, requirement)
        uniqueness_result = self.duplicate_detector.check_uniqueness(test_case, all_test_cases)
        coverage_result = self.coverage_analyzer.assess_coverage(test_case, requirement, all_test_cases)
        
        # Compile results
        validation_results = {
            'test_id': test_case.get('test_id'),
            'requirement_id': test_case.get('requirement_id'),
            'logical_consistency': logical_result,
            'uniqueness': uniqueness_result,
            'coverage_contribution': coverage_result
        }
        
        # Calculate overall confidence
        confidence = self.confidence_scorer.calculate_confidence(validation_results)
        validation_results['confidence'] = confidence
        
        return validation_results
    
    def validate_all(self, test_cases: List[Dict], requirements: List[Dict]) -> Dict:
        """Validate all test cases"""
        
        print("\n" + "="*80)
        print(" " * 25 + "MULTI-LAYER VALIDATION ENGINE")
        print("="*80)
        print(f"\nValidating {len(test_cases)} test cases...")
        
        # Create requirement lookup
        req_lookup = {req['id']: req for req in requirements}
        
        # Validate each test case
        validation_results = []
        high_conf_count = 0
        medium_conf_count = 0
        low_conf_count = 0
        review_count = 0
        
        for i, tc in enumerate(test_cases, 1):
            if i % 50 == 0:
                print(f"  Validated {i}/{len(test_cases)} test cases...")
            
            req = req_lookup.get(tc.get('requirement_id'), {})
            result = self.validate_single(tc, req, test_cases)
            validation_results.append(result)
            
            # Count confidence levels
            conf_level = result['confidence']['confidence_level']
            if conf_level == 'HIGH':
                high_conf_count += 1
            elif conf_level == 'MEDIUM':
                medium_conf_count += 1
            elif conf_level == 'LOW':
                low_conf_count += 1
            else:
                review_count += 1
        
        print(f"  ✅ Validation complete!")
        
        # Calculate aggregate statistics
        avg_confidence = sum(r['confidence']['overall_score'] for r in validation_results) / len(validation_results)
        
        summary = {
            'total_test_cases': len(test_cases),
            'average_confidence': avg_confidence,
            'confidence_distribution': {
                'HIGH (0.8-1.0)': high_conf_count,
                'MEDIUM (0.6-0.8)': medium_conf_count,
                'LOW (0.4-0.6)': low_conf_count,
                'REVIEW_REQUIRED (<0.4)': review_count
            },
            'validation_results': validation_results
        }
        
        # Display summary
        print("\n" + "="*80)
        print("VALIDATION SUMMARY")
        print("="*80)
        print(f"Average Confidence Score: {avg_confidence:.3f}")
        print(f"\nConfidence Distribution:")
        print(f"  🟢 HIGH (0.8-1.0):          {high_conf_count:>4} ({high_conf_count/len(test_cases)*100:.1f}%)")
        print(f"  🟡 MEDIUM (0.6-0.8):        {medium_conf_count:>4} ({medium_conf_count/len(test_cases)*100:.1f}%)")
        print(f"  🟠 LOW (0.4-0.6):           {low_conf_count:>4} ({low_conf_count/len(test_cases)*100:.1f}%)")
        print(f"  🔴 REVIEW_REQUIRED (<0.4):  {review_count:>4} ({review_count/len(test_cases)*100:.1f}%)")
        print("="*80)
        
        return summary
    
    def export_validation_report(self, summary: Dict, output_file: str):
        """Export validation results to Excel"""
        
        print(f"\nExporting validation report to {output_file}...")
        
        validation_results = summary['validation_results']
        
        # Prepare detailed report
        report_data = []
        for result in validation_results:
            report_data.append({
                'test_id': result['test_id'],
                'requirement_id': result['requirement_id'],
                'overall_score': result['confidence']['overall_score'],
                'confidence_level': result['confidence']['confidence_level'],
                'recommendation': result['confidence']['recommendation'],
                'logical_score': result['logical_consistency']['score'],
                'uniqueness_score': result['uniqueness']['score'],
                'coverage_score': result['coverage_contribution']['score'],
                'logical_issues': '; '.join(result['logical_consistency']['issues']),
                'uniqueness_issues': '; '.join(result['uniqueness']['issues']),
                'coverage_contributions': '; '.join(result['coverage_contribution']['contributions'])
            })
        
        df = pd.DataFrame(report_data)
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # Sheet 1: Summary
            summary_df = pd.DataFrame([{
                'Total Test Cases': summary['total_test_cases'],
                'Average Confidence': summary['average_confidence'],
                'High Confidence': summary['confidence_distribution']['HIGH (0.8-1.0)'],
                'Medium Confidence': summary['confidence_distribution']['MEDIUM (0.6-0.8)'],
                'Low Confidence': summary['confidence_distribution']['LOW (0.4-0.6)'],
                'Review Required': summary['confidence_distribution']['REVIEW_REQUIRED (<0.4)']
            }])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Sheet 2: All results
            df.to_excel(writer, sheet_name='Validation Results', index=False)
            
            # Sheet 3: High confidence tests
            high_conf = df[df['confidence_level'] == 'HIGH']
            if not high_conf.empty:
                high_conf.to_excel(writer, sheet_name='High Confidence', index=False)
            
            # Sheet 4: Review required tests
            review = df[df['confidence_level'] == 'REVIEW_REQUIRED']
            if not review.empty:
                review.to_excel(writer, sheet_name='Review Required', index=False)
            
            # Sheet 5: Low uniqueness (potential duplicates)
            low_unique = df[df['uniqueness_score'] < 0.7].sort_values('uniqueness_score')
            if not low_unique.empty:
                low_unique.to_excel(writer, sheet_name='Potential Duplicates', index=False)
        
        print(f"✅ Validation report saved: {output_file}")


# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """Main execution"""
    
    print("\n" + "="*80)
    print(" " * 20 + "TEST CASE VALIDATION PIPELINE")
    print("="*80)
    
    # File paths
    TEST_CASES_FILE = "../04_AI_powered_TestCaseGeneration/optimized_test_cases_20251017_000535.json"
    REQUIREMENTS_FILE = "../03_Chunking_Domain_Understanding/chunked_requirements_with_domain.json"
    VALIDATION_REPORT = "../06_Validation_QA/validation_report.xlsx"
    
    from pathlib import Path
    Path("../06_Validation_QA").mkdir(parents=True, exist_ok=True)
    
    # Load data
    print("\n[1/3] Loading test cases and requirements...")
    with open(TEST_CASES_FILE, 'r', encoding='utf-8') as f:
        test_data = json.load(f)
    
    with open(REQUIREMENTS_FILE, 'r', encoding='utf-8') as f:
        req_data = json.load(f)
    
    # Extract test cases and requirements
    all_test_cases = test_data.get('phase1_test_cases', []) + test_data.get('phase2_test_cases', [])
    
    requirements = []
    for chunk in req_data['chunks']:
        requirements.extend(chunk['requirements'])
    
    # Deduplicate requirements
    seen_ids = set()
    unique_requirements = []
    for req in requirements:
        if req['id'] not in seen_ids:
            unique_requirements.append(req)
            seen_ids.add(req['id'])
    
    print(f"  Loaded {len(all_test_cases)} test cases")
    print(f"  Loaded {len(unique_requirements)} unique requirements")
    
    # Run validation
    print("\n[2/3] Running multi-layer validation...")
    validator = TestCaseValidationEngine()
    summary = validator.validate_all(all_test_cases, unique_requirements)
    
    # Export report
    print("\n[3/3] Exporting validation report...")
    validator.export_validation_report(summary, VALIDATION_REPORT)
    
    print("\n" + "="*80)
    print(" " * 25 + "✅ VALIDATION COMPLETE!")
    print("="*80)
    print(f"\n📊 Final Quality Score: {summary['average_confidence']:.3f}")
    print(f"📁 Validation Report: {VALIDATION_REPORT}")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
