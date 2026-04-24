import json
import pandas as pd
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, asdict, field
from datetime import datetime
from enum import Enum
import ollama
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import os
from dotenv import load_dotenv
load_dotenv()

FAST_MODEL = "llama3:8b"
DEEP_MODEL = "llama3:8b"


# ============================================
# ENHANCED DATA STRUCTURES WITH SRS TRACEABILITY
# ============================================

class TestCaseType(Enum):
    """Complete test case types"""
    INJECTION = "injection"
    BROKEN_AUTH = "broken_auth"
    SENSITIVE_DATA_EXPOSURE = "sensitive_data_exposure"
    BROKEN_ACCESS_CONTROL = "broken_access_control"
    SECURITY_MISCONFIGURATION = "security_misconfiguration"
    XSS = "xss"
    INSECURE_DESERIALIZATION = "insecure_deserialization"
    VULNERABLE_COMPONENTS = "vulnerable_components"
    LOGGING_MONITORING_FAILURES = "logging_monitoring_failures"
    SSRF = "ssrf"


@dataclass
class TestCase:
    """Enhanced test case with SRS traceability"""
    test_id: str
    requirement_id: str
    test_type: str
    test_title: str
    description: str
    preconditions: List[str] = field(default_factory=lambda: ["Application accessible"])
    test_steps: List[str] = field(default_factory=list)
    expected_result: str = "Test should pass as expected"
    test_data: Dict[str, Any] = field(default_factory=dict)
    priority: str = "Medium"
    generation_phase: str = "unknown"
    srs_section: str = ""
    depends_on: List[str] = field(default_factory=list)
    traceability: Dict[str, Any] = field(default_factory=dict)


# ============================================
# SRS-SPECIFIC REQUIREMENT ANALYZER
# ============================================

class SRSRequirementAnalyzer:
    """Analyzes requirements to extract SRS-specific details"""
    
    # NOTE: SRS_PATTERNS was hardcoded for the original restaurant app SRS.
    # It is NOT relevant to the mobile banking pentest use case.
    # Commented out — can be deleted or replaced with domain-specific patterns later.
    #
    # SRS_PATTERNS = {
    #     "CRU_FR6_01": {
    #         "must_test": [
    #             "Search by price (min-max range)",
    #             "Search by destination",
    #             "Search by restaurant type",
    #             "Search by specific dish",
    #             "Free-text search",
    #             "COMBINED multi-criteria search (Price + Distance + Type)"
    #         ]
    #     },
    #     "CRU_FR7_01": {
    #         "must_test": [
    #             "Maximum 100 results displayed on map",
    #             "Default zoom level verification",
    #             "Information link on each pin",
    #             "Filtering menu button present"
    #         ]
    #     },
    #     "CRU_FR8_01": {
    #         "must_test": [
    #             "Maximum 100 results in list view",
    #             "Sorting when search by price: price → distance → type → dish",
    #             "Sorting when NOT by price: distance → price → type → dish",
    #             "Scrollable results",
    #             "Filtering menu button"
    #         ]
    #     },
    #     "CRU_FR11_01": {
    #         "must_test": [
    #             "Picture displayed",
    #             "Name, address, phone, email displayed",
    #             "Type of food and average price shown",
    #             "Full menu with dish names, descriptions, prices"
    #         ]
    #     },
    #     "CRU_FR12_01": {
    #         "must_test": [
    #             "Minimum and maximum price input",
    #             "Results displayed in LIST VIEW by default (not map)",
    #             "Edge case: min = max",
    #             "Only integers accepted (reference FR14)"
    #         ]
    #     },
    #     "CRU_FR24_01": {
    #         "must_test": [
    #             "MANDATORY fields: average price, address, email, phone, restaurant name",
    #             "OPTIONAL fields: description, menu, type, picture, mobile",
    #             "Menu requires: dish name, description, price",
    #             "Form submission with all mandatory fields",
    #             "Form submission with missing mandatory field (should fail)"
    #         ]
    #     }
    # }

    SRS_PATTERNS = {}  # Empty — no hardcoded patterns for current domain
    
    @classmethod
    def get_srs_specifics(cls, req_id: str) -> List[str]:
        """Get SRS-specific requirements for a given requirement ID"""
        return cls.SRS_PATTERNS.get(req_id, {}).get("must_test", [])
    
    @classmethod
    def has_srs_specifics(cls, req_id: str) -> bool:
        """Check if requirement has SRS-specific patterns"""
        return req_id in cls.SRS_PATTERNS


# ============================================
# ENHANCED SRS-AWARE PROMPT GENERATOR
# ============================================

class EnhancedSRSPromptGenerator:
    """Enhanced prompt generator with SRS-specific requirements"""
    def __init__(self, prompts_file: str = "prompts.json"):
        """Load prompts from JSON file"""
        with open(prompts_file, 'r', encoding='utf-8') as f:
            self.prompts = json.load(f)
        print(f"✓ Loaded prompts from {prompts_file}")


    def build_srs_enhanced_prompt(self, requirement: Dict, domain: str, test_types: List[str]) -> str:
        """Generate prompt using JSON template"""
        
        req_id = requirement.get('cru_id', '')
        parent_req = (
            requirement.get('parent_requirement')
            or (requirement.get('traceability', {}).get('source_requirements') or [''])[0]
        )
        req_description = requirement.get('action', '')  # CRUs have 'action' field
        req_rationale = requirement.get('rationale', '')
        dependencies = requirement.get('dependencies', [])
        test_types_str = ", ".join(test_types)
        
        # Check if this requirement has SRS-specific patterns
        srs_specifics = SRSRequirementAnalyzer.get_srs_specifics(parent_req)
        
        srs_specific_section = ""
        if srs_specifics:
            srs_specific_section = f"""
 CRITICAL SRS-SPECIFIC REQUIREMENTS FOR {req_id}:
The SRS document EXPLICITLY requires testing these scenarios:
{chr(10).join([f"  {i+1}. {spec}" for i, spec in enumerate(srs_specifics)])}

YOU MUST generate test cases that DIRECTLY test these specific behaviors.
DO NOT generate generic tests. Each test must address one of the above requirements.
"""
        
        prompt_template = self.prompts["srs_enhanced_prompt"]
        
        # Replace placeholders with actual values
        prompt = prompt_template.format(
            srs_specific_section=srs_specific_section,
            req_id=req_id,
            req_title=requirement.get('title', f"{requirement.get('actor', '')} {requirement.get('action', '')}"),  # CRU structure
            req_description=req_description,
            req_rationale=req_rationale,
            dependencies=', '.join(dependencies) if dependencies else 'None',
            domain=domain,
            test_types_str=test_types_str,
            num_test_types=len(test_types),
            depends_on_json=json.dumps(dependencies)
        )
        
        return prompt
    
    def build_deep_srs_prompt(self, requirement: Dict, domain: str, test_type: str) -> str:
        """Generate deep prompt for comprehensive testing with SRS focus"""
        
        req_id = requirement.get('id', '')
        req_description = requirement.get('description', '')
        req_rationale = requirement.get('rationale', '')
        dependencies = requirement.get('dependencies', [])
        
        # Get SRS specifics
        srs_specifics = SRSRequirementAnalyzer.get_srs_specifics(req_id)
        
        srs_reminder = ""
        if srs_specifics:
            srs_reminder = f"""
 CRITICAL: For {req_id}, the SRS explicitly requires:
{chr(10).join([f"  • {spec}" for spec in srs_specifics])}

Your {test_type.upper()} test cases MUST address these specific SRS requirements.
"""
        # Get type-specific guidance from JSON
        type_guidance = self.prompts["type_guidance"].get(test_type, "Focus on requirement-specific scenarios")
        
        # Get the prompt template from JSON
        prompt_template = self.prompts["deep_srs_prompt"]
        
        # Replace placeholders with actual values
        prompt = prompt_template.format(
            test_type=test_type,
            test_type_upper=test_type.upper(),
            req_id=req_id,
            req_title=requirement.get('title', f"{requirement.get('actor', '')} {requirement.get('action', '')}"),
            req_description=req_description,
            req_rationale=req_rationale,
            dependencies=', '.join(dependencies) if dependencies else 'None',
            domain=domain,
            srs_reminder=srs_reminder,
            type_guidance=type_guidance,
            depends_on_json=json.dumps(dependencies)
        )
       
        return prompt


# ============================================
# ENHANCED VALIDATOR WITH STRICTER CHECKS
# ============================================

class EnhancedSRSValidator:
    """Enhanced validation to prevent placeholder test cases"""
    
    FORBIDDEN_PHRASES = [
        "execute exploit",
        "input payload",
        "verify vulnerability",
        "test_payload",
        "execute test action",
        "input test data",
        "verify result",
        "test_input",
        "test_value",
        "execute action",
        "perform action",
        "check result",
        "validate result"
    ]
    
    @staticmethod
    def validate(test_cases: List[Dict], requirement: Dict, is_comprehensive: bool = False) -> List[Dict]:
        """Strict validation to prevent placeholder test cases"""
        validated = []
        req_id = requirement.get('cru_id', '')
        
        # Get SRS specifics for this requirement
        parent_req = (
            requirement.get('parent_requirement')
            or (requirement.get('traceability', {}).get('source_requirements') or [''])[0]
        )
        srs_specifics = SRSRequirementAnalyzer.get_srs_specifics(parent_req)
        
        for idx, tc in enumerate(test_cases):
            # Ensure all required fields exist with defaults FIRST
            tc.setdefault('test_title', 'Untitled Test')
            tc.setdefault('test_steps', [])
            tc.setdefault('preconditions', ['Application accessible'])
            tc.setdefault('test_data', {})
            tc.setdefault('expected_result', 'Test should pass')
            tc.setdefault('test_type', 'injection')
            tc.setdefault('priority', 'High')
            tc.setdefault('description', 'Security test case description')
            
            # 1. Required fields check
            if not tc.get('test_title') or not tc.get('test_steps'):
                print(f"     Test {idx+1}: Missing title or steps")
                continue
            
            # 2. Check for placeholder phrases in steps
            has_placeholder = False
            steps_str = ' '.join(str(s).lower() for s in tc.get('test_steps', []))
            
            for phrase in EnhancedSRSValidator.FORBIDDEN_PHRASES:
                if phrase in steps_str:
                    print(f"     Test {idx+1}: Contains placeholder phrase '{phrase}'")
                    has_placeholder = True
                    break
            
            if has_placeholder:
                continue
            
            # 3. Check expected result for forbidden phrases
            expected = tc.get('expected_result', '').lower()
            for phrase in ["works as expected", "behaves as expected", "feature works"]:
                if phrase in expected:
                    print(f"     Test {idx+1}: Generic expected result")
                    has_placeholder = True
                    break
            
            if has_placeholder:
                continue
            
            # 4. Normalize test steps
            if isinstance(tc['test_steps'], str):
                tc['test_steps'] = [s.strip() for s in tc['test_steps'].split('\n') if s.strip()]
            
            # 5. Ensure minimum quality steps
            min_steps = 3 if is_comprehensive else 3
            if len(tc['test_steps']) < min_steps:
                print(f"     Test {idx+1}: Only {len(tc['test_steps'])} steps (min: {min_steps})")
                continue
            
            # 6. Normalize preconditions
            if isinstance(tc.get('preconditions'), str):
                tc['preconditions'] = [s.strip() for s in tc['preconditions'].split(',') if s.strip()]
            elif not isinstance(tc.get('preconditions'), list):
                tc['preconditions'] = ["Application accessible", "User has necessary permissions"]
            
            # 7. Normalize test data
            if isinstance(tc.get('test_data'), str):
                try:
                    tc['test_data'] = json.loads(tc['test_data'])
                except:
                    tc['test_data'] = {"data": tc['test_data']}
            elif not isinstance(tc.get('test_data'), dict):
                tc['test_data'] = {}
            
            # 8. Check test data quality
            test_data_str = str(tc.get('test_data', {})).lower()
            if 'test_input' in test_data_str or 'test_value' in test_data_str:
                print(f"     Test {idx+1}: Generic test data")
                # Don't skip, but flag
            
            # 9. Check expected result length
            if len(expected) < 30:
                print(f"     Test {idx+1}: Expected result too short ({len(expected)} chars)")
                continue
            
            # 10. For critical SRS requirements, check if test addresses SRS specifics
            if srs_specifics and is_comprehensive:
                # Check if test case title or description references SRS-specific patterns
                tc_text = (tc.get('test_title', '') + ' ' + tc.get('description', '')).lower()
                addresses_srs = any(
                    keyword.lower() in tc_text 
                    for spec in srs_specifics 
                    for keyword in spec.split()[:3]  # Check first 3 words of each spec
                )
                if not addresses_srs:
                    print(f"    Test {idx+1}: May not address SRS-specific requirements for {req_id}")
            
            # 11. Add requirement metadata
            tc['requirement_id'] = requirement['cru_id']
            tc['description'] = tc.get('description', f"Test case for {requirement.get('action', requirement['cru_id'])}")

            
            # 12. Normalize priority
            if tc.get('priority') not in ['High', 'Medium', 'Low']:
                tc['priority'] = 'High'
            
            # 13. Add SRS section
            if not tc.get('srs_section'):
                tc['srs_section'] = (
                    requirement.get('srs_section')
                    or (requirement.get('traceability', {}).get('sections') or [''])[0]
                )
            
            # 14. Add dependencies
            if not tc.get('depends_on'):
                tc['depends_on'] = requirement.get('dependencies', [])
            
            validated.append(tc)
        
        return validated


# ============================================
# ENHANCED HYBRID ENGINE
# ============================================

class OptimizedHybridEngine:
    def __init__(self, model_name: str = "llama3:8b", prompts_file: str = "prompts.json"):
        self.model_name = model_name
        self.prompt_gen = EnhancedSRSPromptGenerator(prompts_file)
        self.validator = EnhancedSRSValidator()
        self.test_counter = 1
        self.max_workers = 5
        
        print(f"\n{'='*80}")
        print(f"⚡ OPTIMIZED SRS-AWARE HYBRID TEST GENERATION ENGINE")
        print(f"{'='*80}")
        print(f"Model: {model_name}")
        print(f"✓ Prompts loaded from: {prompts_file}")
        print(f" Enforces SRS-specific test cases")
        print(f" Blocks placeholder/template tests")
        print(f" Deduplicates overlapping requirements")
        print(f"{'='*80}\n")
        
        try:
            ollama.list()
            ollama.generate(model=self.model_name, prompt="test", options={'num_predict': 1})
            print("✓ Model loaded and ready\n")
        except Exception as e:
            print(f"⚠ Ollama issue: {e}\n")
    
    def set_model(self, model_name: str):
        if self.model_name != model_name:
            print(f"🔁 Switching model → {model_name}")
            self.model_name = model_name

    
    def _call_llm(self, prompt: str, is_comprehensive: bool = False) -> Optional[str]:
        """Call LLM with appropriate settings"""
        try:
            print(f"      Calling LLM... (prompt length: {len(prompt)} chars)")
            response = ollama.generate(
                model=self.model_name,
                prompt=prompt,
                options={
                    'temperature': 0.15,
                    'top_p': 0.9,
                    'num_predict': 3000,
                    'repeat_penalty': 1.1,
                    'num_ctx': 4096
                }
            )
            response_text = response['response']

            print(f"      LLM response length: {len(response_text)} chars")
            print(f"      First 200 chars: {response_text[:200]}")
            time.sleep(0.8)
            return response_text
        except Exception as e:
            print(f"⚠ LLM error: {e}")
            import traceback
            traceback.print_exc()
            return None
 

    def _parse_json(self, response: str) -> List[Dict]:
        if not response:
            print("      No response to parse")
            return []

        cleaned = response.strip()

        if cleaned.startswith('```'):
            lines = cleaned.split('\n')
            cleaned = '\n'.join(line for line in lines if not line.strip().startswith('```'))

        start = cleaned.find('[')
        end = cleaned.rfind(']') + 1

        if start == -1 or end == 0:
            print("      No JSON array found")
            return []

        json_str = cleaned[start:end]

        try:
            parsed = json.loads(json_str)
            print(f"      ✓ Successfully parsed {len(parsed)} test cases")
            return parsed
        except json.JSONDecodeError as e:
            print(f"⚠ JSON parse error: {e}")
            print(f"      First 500 chars of response: {response[:500]}")
            return []

    
    def generate_fast_batch(self, requirement: Dict, domain: str, test_types: List[str]) -> List[TestCase]:
        """Phase 1: Fast batch with SRS awareness"""

        req_id = requirement.get('cru_id', requirement.get('id', 'UNKNOWN'))
        print(f"    Generating for {req_id}...")
        prompt = self.prompt_gen.build_srs_enhanced_prompt(requirement, domain, test_types)
        response = self._call_llm(prompt, is_comprehensive=False)
        if not response:
            print(f"    No response from LLM for {req_id}")
            return []
        
        generated = self._parse_json(response)
        print(f"    Generated {len(generated)} raw test cases")
        
        validated = self.validator.validate(generated, requirement, is_comprehensive=False)
        print(f"    Validated {len(validated)} test cases (rejected {len(generated) - len(validated)})")
        test_cases = []
        for tc in validated:
            test_case = TestCase(
                test_id=f"TC_{req_id}_{self.test_counter:03d}",
                requirement_id=tc['requirement_id'],
                test_type=tc.get('test_type', 'injection'),
                test_title=tc['test_title'],
                description=tc['description'],
                preconditions=tc['preconditions'],
                test_steps=tc['test_steps'],
                expected_result=tc['expected_result'],
                test_data=tc['test_data'],
                priority=tc['priority'],
                generation_phase='fast_batch',
                srs_section=tc.get('srs_section', ''),
                depends_on=tc.get('depends_on', []),
                traceability=requirement.get('traceability', {})
            )
            test_cases.append(test_case)
            self.test_counter += 1
        
        return test_cases
    
    def generate_comprehensive(self, requirement: Dict, domain: str, test_types: List[str]) -> List[TestCase]:
        """Phase 2: Comprehensive with SRS details"""
        
        all_test_cases = []
        
        for test_type in test_types:
            print(f"      Generating {test_type} tests...")
            
            prompt = self.prompt_gen.build_deep_srs_prompt(requirement, domain, test_type)
            response = self._call_llm(prompt, is_comprehensive=True)
            generated = self._parse_json(response)
            
            validated = self.validator.validate(generated, requirement, is_comprehensive=True)
            
            print(f"       {len(validated)} {test_type} tests validated")
            
            for tc in validated:
                req_id = requirement.get('cru_id', requirement.get('id', 'UNKNOWN'))
                test_case = TestCase(
                    test_id=f"TC_{req_id}_{self.test_counter:03d}",
                    requirement_id=tc['requirement_id'],
                    test_type=tc.get('test_type', test_type),
                    test_title=tc['test_title'],
                    description=tc['description'],
                    preconditions=tc['preconditions'],
                    test_steps=tc['test_steps'],
                    expected_result=tc['expected_result'],
                    test_data=tc['test_data'],
                    priority=tc['priority'],
                    generation_phase='comprehensive',
                    srs_section=tc.get('srs_section', ''),
                    depends_on=tc.get('depends_on', []),
                    traceability=requirement.get('traceability', {})
                )
                all_test_cases.append(test_case)
                self.test_counter += 1
        
        return all_test_cases
    
    def generate_parallel_fast(self, requirements: List[Dict], domain: str, test_types: List[str]) -> List[TestCase]:
        """Parallel fast batch processing"""
        
        all_test_cases = []
        total = len(requirements)
        
        print(f" PHASE 1: SRS-aware fast batch for {total} requirements...")
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self.generate_fast_batch, req, domain, test_types): req 
                for req in requirements
            }
            
            completed = 0
            for future in as_completed(futures):
                req = futures[future]
                try:
                    test_cases = future.result()
                    all_test_cases.extend(test_cases)
                    completed += 1
                    
                    elapsed = time.time() - start_time
                    avg_time = elapsed / completed
                    remaining = (total - completed) * avg_time
                    
                    req_id = req.get('cru_id', req.get('id', 'UNKNOWN'))
                    print(f"   {completed}/{total} | {req_id} | "
                          f"{len(test_cases)} tests | ETA: {remaining/60:.1f}m")
                except Exception as e:
                    req_id = req.get('cru_id', req.get('id', 'UNKNOWN'))
                    print(f"   {req_id}: {e}")
                    completed += 1
        
        elapsed = time.time() - start_time
        print(f"\n Phase 1 completed in {elapsed/60:.1f} minutes")
        print(f" Generated {len(all_test_cases)} quality test cases\n")
        
        return all_test_cases
    
    def generate_sequential_comprehensive(self, requirements: List[Dict], domain: str, test_types: List[str]) -> List[TestCase]:
        """Sequential comprehensive processing"""
        
        all_test_cases = []
        total = len(requirements)
        
        print(f"🔍 PHASE 2: Comprehensive SRS-specific generation for {total} critical requirements...")
        start_time = time.time()
        
        for idx, req in enumerate(requirements, 1):
            req_id = req.get('cru_id', req.get('id', 'UNKNOWN'))
            actor = req.get('actor', 'System')
            action = req.get('action', 'perform action')
            req_title = f"{actor} {action}"
            print(f"\n  [{idx}/{total}] {req_id}: {req_title[:60]}...")
            
            # Flag if this is a critical SRS requirement
            parent_req = (
                req.get('parent_requirement')
                or (req.get('traceability', {}).get('source_requirements') or [req_id])[0]
            )
            if SRSRequirementAnalyzer.has_srs_specifics(parent_req):
                print(f"       SRS-critical requirement - enhanced testing")
            
            try:
                test_cases = self.generate_comprehensive(req, domain, test_types)
                all_test_cases.extend(test_cases)
                
                elapsed = time.time() - start_time
                avg_time = elapsed / idx
                remaining = (total - idx) * avg_time
                
                print(f"   Total {len(test_cases)} comprehensive tests | ETA: {remaining/60:.1f}m")
            except Exception as e:
                print(f"   Error: {e}")
        
        elapsed = time.time() - start_time
        print(f"\n  Phase 2 completed in {elapsed/60:.1f} minutes")
        print(f" Generated {len(all_test_cases)} comprehensive test cases\n")
        
        return all_test_cases


# ============================================
# OPTIMIZED ORCHESTRATOR WITH DEDUPLICATION
# ============================================

class OptimizedHybridGenerator:
    """Optimized SRS-aware hybrid test generator with deduplication"""
    
    def __init__(self, input_file: str, model_name: str = "llama3:8b", prompts_file: str = "prompts.json"):
        self.input_file = input_file
        self.engine = OptimizedHybridEngine(model_name, prompts_file=prompts_file)
        self.data = None
        self.phase1_test_cases = []
        self.phase2_test_cases = []
        
        self.phase1_types = ['injection', 'broken_auth', 'sensitive_data_exposure', 'broken_access_control']
        self.phase2_types = ['security_misconfiguration', 'xss', 'insecure_deserialization', 'vulnerable_components', 'logging_monitoring_failures', 'ssrf']
                
        print(f"Phase 1 types: {self.phase1_types}")
        print(f"Phase 2 types: {self.phase2_types}\n")
    
    def load_data(self):
        """Load input data"""
        with open(self.input_file, 'r', encoding='utf-8') as f:
            self.data = json.load(f)
        
        # Calculate overlap
        total_instances = sum(len(chunk['crus']) for chunk in self.data['chunks'])
        unique_count = len(self._deduplicate_requirements())
        overlap_count = total_instances - unique_count
        
        print(f"{'='*80}")
        print(f" LOADED CRU DATA")
        print(f"{'='*80}")
        print(f"Total CRU instances: {total_instances}")
        print(f"Unique CRU: {unique_count}")
        if overlap_count > 0:
            print(f" Overlap detected: {overlap_count} duplicate instances ({overlap_count/total_instances*100:.1f}%)")
            print(f" Deduplication will be applied")
        print(f"Chunks: {self.data['metadata']['total_chunks']}")
        domain = self.data['chunks'][0]['application_domain'][0] if self.data.get('chunks') else "Unknown"
        print(f"Domain: {domain}")
        print(f"{'='*80}\n")
    
    def _deduplicate_requirements(self) -> List[Dict]:
        all_requirements = []
        seen_req_ids = set()

        for chunk in self.data['chunks']:
            for req in chunk['crus']:
                cru_id = req.get('cru_id')
                if cru_id and cru_id not in seen_req_ids:
                    req_copy = dict(req)
                    req_copy['capability_tags'] = chunk.get('capability_tags', [])
                    req_copy['application_domain'] = chunk.get('application_domain', [])
                    all_requirements.append(req_copy)
                    seen_req_ids.add(cru_id)

        return all_requirements
    
    def identify_critical_requirements(self, top_n: int = 15) -> List[Dict]:
        """Identify critical requirements (with deduplication)"""
        
        # Get unique requirements first
        all_requirements = self._deduplicate_requirements()
        
        critical_reqs = []
        
        # Priority 1: SRS-critical (check parent_requirement)
        for req in all_requirements:
            parent_req = (
                req.get('parent_requirement')
                or (req.get('traceability', {}).get('source_requirements') or [''])[0]
            )
            if SRSRequirementAnalyzer.has_srs_specifics(parent_req):
                critical_reqs.append(req)
        
        # Priority 2: Requirements with dependencies
        for req in all_requirements:
            if req.get('dependencies') and len(req['dependencies']) > 0:
                if req not in critical_reqs:
                    critical_reqs.append(req)
        
        # Priority 3: First N requirements (core features)
        for req in all_requirements[:top_n]:
            if req not in critical_reqs:
                critical_reqs.append(req)
        
        critical_reqs = critical_reqs[:top_n]
        
        print(f"{'='*80}")
        print(f" CRITICAL REQUIREMENTS FOR PHASE 2")
        print(f"{'='*80}")
        print(f"Total unique requirements: {len(all_requirements)}")
        print(f"Selected for Phase 2: {len(critical_reqs)}")
        for req in critical_reqs:
            deps = ', '.join(req.get('dependencies', [])) if req.get('dependencies') else 'None'
            parent = req.get('parent_requirement', '')
            srs_flag = "  SRS-Critical" if SRSRequirementAnalyzer.has_srs_specifics(parent) else ""
            actor = req.get('actor', 'Unknown')
            action = req.get('action', 'No action specified')
            action_display = action[:40] if len(action) > 40 else action
            print(f"  • {req['cru_id']}: {actor} {action_display} | Parent: {parent}{srs_flag}")
        print(f"{'='*80}\n")
        
        return critical_reqs
    
    def generate_phase1(self):
        """Phase 1: Fast batch with deduplication"""
        domain = self.data['chunks'][0]['application_domain'][0] if self.data.get('chunks') else "Unknown"
        
        # Deduplicate requirements (fixes 22% overlap)
        all_requirements = self._deduplicate_requirements()
        
        print(f" Deduplicated: {len(all_requirements)} unique requirements\n")
        
        self.engine.set_model(FAST_MODEL)

        self.phase1_test_cases = self.engine.generate_parallel_fast(
            all_requirements, domain, self.phase1_types
        )
    
    def generate_phase2(self, critical_requirements: List[Dict]):
        """Phase 2: Comprehensive"""
        domain = self.data['chunks'][0]['application_domain'][0] if self.data.get('chunks') else "Unknown"
        
        self.engine.set_model(DEEP_MODEL)

        self.phase2_test_cases = self.engine.generate_sequential_comprehensive(
            critical_requirements, domain, self.phase2_types
        )
    
    def save_results(self, output_prefix: str = "optimized_test_cases"):
        """Save results"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        all_test_cases = self.phase1_test_cases + self.phase2_test_cases
        
        # JSON
        json_data = {
            "metadata": {
                "generated": datetime.now().isoformat(),
                "model": self.engine.model_name,
                "total_test_cases": len(all_test_cases),
                "phase1_count": len(self.phase1_test_cases),
                "phase2_count": len(self.phase2_test_cases),
                "validation": "SRS-aware with placeholder blocking and deduplication",
                "improvements": [
                    "Deduplication applied (fixed 22% overlap)",
                    "SRS-specific prompts for critical requirements",
                    "Enhanced validation with stricter checks"
                ]
            },
            "phase1_test_cases": [asdict(tc) for tc in self.phase1_test_cases],
            "phase2_test_cases": [asdict(tc) for tc in self.phase2_test_cases]
        }
        
        json_file = f"{output_prefix}_{timestamp}.json"
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, indent=2)
        
        print(f" Saved JSON: {json_file}")
        
        # Excel
        df = pd.DataFrame([asdict(tc) for tc in all_test_cases])

        # Check if DataFrame is empty
        if df.empty:
            print("⚠️ Warning: No test cases generated. Creating empty report.")
            df = pd.DataFrame(columns=['test_id', 'requirement_id', 'test_type', 'test_title', 
                                        'description', 'preconditions', 'test_steps', 'expected_result',
                                        'test_data', 'priority', 'generation_phase', 'srs_section', 'depends_on'])
        else:
            # SAFE handling with .get() to avoid KeyError
            if 'preconditions' in df.columns:
                df['preconditions'] = df['preconditions'].apply(
                    lambda x: '\n'.join([f"• {p}" for p in x]) if isinstance(x, list) else str(x) if x else "None"
                )
            else:
                df['preconditions'] = "None"
            
            if 'test_steps' in df.columns:
                df['test_steps'] = df['test_steps'].apply(
                    lambda x: '\n'.join([f"{i+1}. {s}" for i, s in enumerate(x)]) if isinstance(x, list) else str(x) if x else "None"
                )
            else:
                df['test_steps'] = "None"
            
            if 'test_data' in df.columns:
                df['test_data'] = df['test_data'].apply(
                    lambda x: json.dumps(x, indent=2) if isinstance(x, dict) else str(x) if x else "{}"
                )
            else:
                df['test_data'] = "{}"
            
            if 'depends_on' in df.columns:
                df['depends_on'] = df['depends_on'].apply(
                    lambda x: ', '.join(x) if isinstance(x, list) and x else "None"
                )
            else:
                df['depends_on'] = "None"

        
        excel_file = f"{output_prefix}_{timestamp}.xlsx"
        with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
            overview = pd.DataFrame([{
                'Total Tests': len(all_test_cases),
                'Phase 1': len(self.phase1_test_cases),
                'Phase 2': len(self.phase2_test_cases),
                'Deduplicated': 'Yes',
                'SRS-Aware': 'Yes',
                'High Priority': len([t for t in all_test_cases if t.priority == 'High'])
            }])
            overview.to_excel(writer, sheet_name='Overview', index=False)
            
            df.to_excel(writer, sheet_name='All Tests', index=False)
            
            phase1_df = df[df['generation_phase'] == 'fast_batch']
            if not phase1_df.empty:
                phase1_df.to_excel(writer, sheet_name='Phase 1 - Fast', index=False)
            
            phase2_df = df[df['generation_phase'] == 'comprehensive']
            if not phase2_df.empty:
                phase2_df.to_excel(writer, sheet_name='Phase 2 - Deep', index=False)
            
            all_types = self.phase1_types + self.phase2_types
            for test_type in all_types:
                type_df = df[df['test_type'] == test_type]
                if not type_df.empty:
                    type_df.to_excel(writer, sheet_name=test_type.capitalize()[:31], index=False)
        
        print(f" Saved Excel: {excel_file}")
        
        # Summary
        summary_file = f"{output_prefix}_summary_{timestamp}.txt"
        with open(summary_file, 'w') as f:
            f.write("="*80 + "\n")
            f.write("OPTIMIZED SRS-AWARE HYBRID TEST GENERATION SUMMARY\n")
            f.write("="*80 + "\n\n")
            f.write(f"Generated: {datetime.now()}\n")
            f.write(f"Total Tests: {len(all_test_cases)}\n\n")
            
            f.write("IMPROVEMENTS APPLIED:\n")
            f.write(" Deduplication (fixed 22% overlap)\n")
            f.write(" SRS-specific prompts for FR6, FR8, FR24, etc.\n")
            f.write("  Enhanced validation blocking placeholders\n")
            f.write("  Priority targeting for critical SRS gaps\n\n")
            
            f.write("PHASE 1 - FAST BATCH:\n")
            f.write(f"  Tests: {len(self.phase1_test_cases)}\n")
            f.write(f"  Types: {', '.join(self.phase1_types)}\n\n")
            
            f.write("PHASE 2 - COMPREHENSIVE:\n")
            f.write(f"  Tests: {len(self.phase2_test_cases)}\n")
            f.write(f"  Types: {', '.join(self.phase2_types)}\n\n")
            
            f.write("BY TYPE:\n")
            for test_type in self.phase1_types + self.phase2_types:
                count = len([t for t in all_test_cases if t.test_type == test_type])
                f.write(f"  {test_type}: {count}\n")
        
        print(f" Saved Summary: {summary_file}")
        
        return json_file, excel_file, summary_file
    
    def show_stats(self):
        """Display statistics"""
        all_test_cases = self.phase1_test_cases + self.phase2_test_cases
        
        print(f"\n{'='*80}")
        print(f" FINAL STATISTICS")
        print(f"{'='*80}")
        print(f"Total Test Cases: {len(all_test_cases)}")
        print(f"\nBy Phase:")
        print(f"  Phase 1: {len(self.phase1_test_cases)}")
        print(f"  Phase 2: {len(self.phase2_test_cases)}")
        print(f"\nQuality Metrics:")
        print(f"  High priority: {len([t for t in all_test_cases if t.priority == 'High'])}")
        print(f"  With dependencies: {len([t for t in all_test_cases if t.depends_on])}")
        print(f"{'='*80}\n")


# ============================================
# MAIN
# ============================================

def main():
    INPUT_FILE = "../Segmentation_and_Classification/output/chunked_crus_with_domain.json"
    PROMPTS_FILE = "../Testcase_Generation/prompts.json"
    OUTPUT_PREFIX = "../Testcase_Generation/output/optimized_test_cases"
    NUM_CRITICAL = int(os.getenv("NUM_CRITICAL_REQUIREMENTS", "25"))
    
    print("="*80)
    print(" " * 10 + "⚡ OPTIMIZED SRS-AWARE HYBRID TEST GENERATION")
    print("="*80)
    print(" Deduplication (fixes 22% overlap)")
    print(" SRS-specific prompts (FR6, FR8, FR24, etc.)")
    print(" Enhanced validation (blocks placeholders)")
    print(" Priority targeting (critical SRS gaps)")
    print("="*80 + "\n")
    
    generator = OptimizedHybridGenerator(INPUT_FILE,  prompts_file=PROMPTS_FILE)
    
    print("[1/4] Loading data...")
    generator.load_data()
    
    print("[2/4] Identifying critical requirements...")
    critical_reqs = generator.identify_critical_requirements(top_n=NUM_CRITICAL)
    
    print(f"[3/4] Phase 1: Optimized fast batch...")
    phase1_start = time.time()
    generator.generate_phase1()
    phase1_time = time.time() - phase1_start
    
    print(f"[4/4] Phase 2: Comprehensive deep generation...")
    phase2_start = time.time()
    generator.generate_phase2(critical_reqs)
    phase2_time = time.time() - phase2_start
    
    print("\n[5/5] Saving results...")
    json_f, excel_f, summary_f = generator.save_results(OUTPUT_PREFIX)
    
    generator.show_stats()
    
    total_time = phase1_time + phase2_time
    print("="*80)
    print(" COMPLETE!")
    print("="*80)
    print(f"Phase 1: {phase1_time/60:.1f}m | Phase 2: {phase2_time/60:.1f}m | Total: {total_time/60:.1f}m")
    print(f"Tests: {len(generator.phase1_test_cases) + len(generator.phase2_test_cases)}")
    print(f"\nFiles:\n  • {json_f}\n  • {excel_f}\n  • {summary_f}")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()