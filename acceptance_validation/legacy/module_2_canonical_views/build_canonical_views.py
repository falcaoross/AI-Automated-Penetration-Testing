"""
Module 5.2: Canonical Acceptance View Builder

This module transforms flat acceptance units into structured canonical scenario views.
It performs STRUCTURAL transformation only - no semantic reasoning, no comparison, no inference.

Core Responsibility:
    Group acceptance units by scenario_id and organize steps into preconditions/actions/outcomes
    based on strict structural rules (Gherkin prefix detection).

Constraints:
    - Preserves exact text (no paraphrasing)
    - No semantic interpretation
    - No validation against CRUs
    - No similarity matching
    - Pure structural grouping
    - Deterministic behavior
"""

import json
import re
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from collections import defaultdict


class CanonicalView:
    """Represents a structured canonical acceptance view."""
    
    def __init__(
        self,
        scenario_id: str,
        scenario_title: Optional[str] = None,
        user_story: Optional[str] = None
    ):
        self.scenario_id = scenario_id
        self.scenario_title = scenario_title
        self.user_story = user_story
        self.preconditions: List[str] = []
        self.actions: List[str] = []
        self.outcomes: List[str] = []
        self.postconditions: List[str] = []
        self.source_units: List[str] = []
    
    def to_dict(self) -> Dict:
        """Convert to output JSON schema."""
        # Determine if view has executable steps
        has_executable_steps = bool(
            self.preconditions or
            self.actions or
            self.outcomes or
            self.postconditions
        )
        
        return {
            "scenario_id": self.scenario_id,
            "scenario_title": self.scenario_title,
            "user_story": self.user_story,
            "preconditions": self.preconditions,
            "actions": self.actions,
            "outcomes": self.outcomes,
            "postconditions": self.postconditions,
            "has_executable_steps": has_executable_steps,
            "traceability": {
                "source_units": self.source_units
            }
        }


class CanonicalViewBuilder:
    """Main builder for canonical acceptance views."""
    
    def __init__(self, input_file: str):
        self.input_file = input_file
        self.views: List[CanonicalView] = []
        self.standalone_counter = 0
        
    def load_acceptance_units(self) -> Dict:
        """Load acceptance units from JSON."""
        with open(self.input_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def extract_scenario_title(self, text: str) -> str:
        """
        Extract scenario title from header text.
        Remove 'Scenario:' prefix only. Do NOT rewrite anything else.
        """
        # Remove common scenario prefixes
        text = re.sub(r'^(Scenario|Scenario Outline|Acceptance Scenario):\s*', '', text, flags=re.IGNORECASE)
        return text.strip()
    
    def classify_step(self, text: str, current_category: Optional[str]) -> str:
        """
        Classify step structurally based on Gherkin prefix.
        
        Rules:
        - "Given" → preconditions
        - "When" → actions
        - "Then" → outcomes
        - "And" → attach to most recent category
        
        No semantic reasoning.
        """
        text_stripped = text.strip()
        
        # Check explicit prefixes
        if re.match(r'^Given\b', text_stripped, re.IGNORECASE):
            return 'preconditions'
        
        if re.match(r'^When\b', text_stripped, re.IGNORECASE):
            return 'actions'
        
        if re.match(r'^Then\b', text_stripped, re.IGNORECASE):
            return 'outcomes'
        
        # "And" attaches to most recent category
        if re.match(r'^And\b', text_stripped, re.IGNORECASE):
            if current_category:
                return current_category
            # If no current category, default to outcomes
            return 'outcomes'
        
        # Default fallback: treat as postcondition if no clear prefix
        return 'postconditions'
    
    def group_by_scenario(self, units: List[Dict]) -> Dict[str, List[Dict]]:
        """
        Group acceptance units by scenario_id.
        Handle standalone user stories with synthetic IDs.
        
        Adjacency rule:
        - If a user_story (scenario_id=null) is immediately followed by a scenario header,
          attach the user_story to that scenario instead of creating standalone view.
        - Only create standalone if not followed by a header.
        """
        grouped = defaultdict(list)
        
        for i, unit in enumerate(units):
            scenario_id = unit.get('scenario_id')
            
            # Handle standalone user stories (scenario_id = null)
            if scenario_id is None:
                if unit.get('source_type') == 'user_story':
                    # Check if next unit is a scenario header
                    next_is_header = False
                    next_scenario_id = None
                    
                    if i + 1 < len(units):
                        next_unit = units[i + 1]
                        if next_unit.get('is_header'):
                            next_is_header = True
                            next_scenario_id = next_unit.get('scenario_id')
                    
                    # If followed by header, attach to that scenario
                    if next_is_header and next_scenario_id:
                        grouped[next_scenario_id].append(unit)
                    else:
                        # Create standalone view
                        self.standalone_counter += 1
                        synthetic_id = f"SCN_STANDALONE_{self.standalone_counter:03d}"
                        unit['_synthetic_scenario_id'] = synthetic_id
                        grouped[synthetic_id].append(unit)
                else:
                    # Units without scenario_id and not user_story are skipped
                    # (shouldn't happen with proper Module 5.1 output)
                    continue
            else:
                grouped[scenario_id].append(unit)
        
        return grouped
    
    def build_view_from_units(self, scenario_id: str, units: List[Dict]) -> CanonicalView:
        """
        Build a canonical view from grouped units.
        
        Logic:
        1. Find header (is_header=true) → extract scenario_title
        2. Find user_story (source_type=user_story) → attach as user_story
        3. Classify steps structurally by prefix
        4. Preserve traceability
        """
        view = CanonicalView(scenario_id=scenario_id)
        
        # Track current category for "And" steps
        current_category = None
        
        for unit in units:
            uas_id = unit['uas_id']
            text = unit['text']
            is_header = unit.get('is_header', False)
            source_type = unit.get('source_type')
            
            # Add to traceability
            view.source_units.append(uas_id)
            
            # Extract scenario title from header
            if is_header:
                view.scenario_title = self.extract_scenario_title(text)
                continue
            
            # Attach user story
            if source_type == 'user_story':
                view.user_story = text
                continue
            
            # Classify and add step
            category = self.classify_step(text, current_category)
            current_category = category
            
            if category == 'preconditions':
                view.preconditions.append(text)
            elif category == 'actions':
                view.actions.append(text)
            elif category == 'outcomes':
                view.outcomes.append(text)
            elif category == 'postconditions':
                view.postconditions.append(text)
        
        return view
    
    def build_views(self):
        """Main orchestration."""
        data = self.load_acceptance_units()
        units = data.get('acceptance_units', [])
        
        # Group by scenario_id
        grouped = self.group_by_scenario(units)
        
        # Build canonical view for each scenario
        for scenario_id in sorted(grouped.keys()):
            scenario_units = grouped[scenario_id]
            view = self.build_view_from_units(scenario_id, scenario_units)
            self.views.append(view)
    
    def to_json(self, output_path: str):
        """Export canonical views to JSON."""
        import os
        
        # Get directory where this script is located
        base_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Create output directory inside script folder
        output_dir = os.path.join(base_dir, "output")
        os.makedirs(output_dir, exist_ok=True)
        
        # Save file inside that directory
        output_path = os.path.join(output_dir, output_path)
        
        output = {
            "metadata": {
                "total_views": len(self.views),
                "generated_from": "acceptance_units.json",
                "generation_timestamp": datetime.utcnow().isoformat() + "Z"
            },
            "canonical_views": [view.to_dict() for view in self.views]
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        return output


def build_canonical_acceptance_views(
    input_file: str,
    output_file: str = "canonical_acceptance_views.json"
) -> Dict:
    """Main entry point for Module 5.2."""
    print(f"\n{'='*70}")
    print("MODULE 5.2: Canonical Acceptance View Builder")
    print(f"{'='*70}\n")
    
    builder = CanonicalViewBuilder(input_file)
    
    print(f"📄 Processing: {input_file}")
    print(f"🔍 Building canonical views...")
    
    builder.build_views()
    
    print(f"\n✅ Build Complete")
    print(f"   • Total Views: {len(builder.views)}")
    
    # Count scenario types
    regular_scenarios = sum(1 for v in builder.views if not v.scenario_id.startswith('SCN_STANDALONE'))
    standalone_stories = sum(1 for v in builder.views if v.scenario_id.startswith('SCN_STANDALONE'))
    
    print(f"   • Regular Scenarios: {regular_scenarios}")
    print(f"   • Standalone User Stories: {standalone_stories}")
    
    # Summary statistics
    total_preconditions = sum(len(v.preconditions) for v in builder.views)
    total_actions = sum(len(v.actions) for v in builder.views)
    total_outcomes = sum(len(v.outcomes) for v in builder.views)
    total_postconditions = sum(len(v.postconditions) for v in builder.views)
    
    print(f"\n📊 Step Distribution:")
    print(f"   • Preconditions: {total_preconditions}")
    print(f"   • Actions: {total_actions}")
    print(f"   • Outcomes: {total_outcomes}")
    print(f"   • Postconditions: {total_postconditions}")
    
    result = builder.to_json(output_file)
    
    print(f"\n💾 Output: {output_file}")
    print(f"{'='*70}\n")
    
    return result


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python build_canonical_views.py <input_file> [output_file]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "canonical_acceptance_views.json"
    
    build_canonical_acceptance_views(input_file, output_file)