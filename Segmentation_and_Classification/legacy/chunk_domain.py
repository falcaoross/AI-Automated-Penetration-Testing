"""
Stage 4: Semantic Chunking & Capability Tagging
Pure enrichment and packaging layer between CRU normalization and test generation

Purpose:
- Group CRUs into semantically cohesive chunks
- Attach application domain (from fixed vocabulary)
- Attach capability tags (lightweight labels from CRU content)
- Prepare LLM-friendly payloads
- Preserve full traceability

This module does NOT reason, infer, or invent semantics.
It only organizes and labels what already exists in CRUs.
"""

import json
from typing import List, Dict, Any, Tuple
from collections import defaultdict
from pathlib import Path
from domains import APPLICATION_DOMAINS


# ======================
# Configuration
# ======================

# Application domain - ONE per project/run (not inferred per chunk)
# Override via CLI argument or set here
DEFAULT_APPLICATION_DOMAIN = "Task Management / Productivity Tools"

CONFIG = {
    "max_crus_per_chunk": 5,
    "min_crus_per_chunk": 2,
}

# Capability tag patterns derived from CRU types and actions
# These are CAPABILITY/QUALITY labels - NOT application domains
# These are LABELS ONLY - no compliance, no business logic
CAPABILITY_TAG_PATTERNS = {
    "Authentication": ["authenticate", "login", "signup", "register", "establishes account", "validates hash"],
    "CRUD Operations": ["create", "update", "delete", "insert", "edit", "remove"],
    "Data Management": ["persist", "store", "fetch", "query", "ensures data integrity"],
    "Filtering": ["filter", "search", "query"],
    "Performance": ["handle users", "response time", "latency", "concurrent"],
    "Security": ["hash", "encrypt", "validate", "credential", "auth"],
    "Reliability": ["uptime", "backup", "recovery", "causes outage"],
    "Portability": ["compatibility", "browser", "platform", "OS"],
    "User Interface": ["display", "render", "show", "UI"],
}


# ======================
# CRU Chunking
# ======================

def group_crus_by_requirement(crus: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group CRUs by parent requirement ID."""
    grouped = defaultdict(list)
    for cru in crus:
        parent_id = cru.get("parent_requirement_id", "UNKNOWN")
        grouped[parent_id].append(cru)
    return dict(grouped)


def group_crus_by_type(crus: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group CRUs by type (functional, performance, etc.)."""
    grouped = defaultdict(list)
    for cru in crus:
        cru_type = cru.get("type", "other")
        grouped[cru_type].append(cru)
    return dict(grouped)


def create_chunks(crus: List[Dict[str, Any]], application_domain: str) -> List[Dict[str, Any]]:
    """Create semantically cohesive chunks from CRUs.
    
    Chunking strategy:
    1. Group by parent requirement (natural semantic boundary)
    2. Group by type if same parent
    3. Respect max chunk size
    
    Args:
        crus: List of CRU dictionaries
        application_domain: Single application domain for all chunks
        
    Returns:
        List of chunk dictionaries
    """
    print("\n🔍 Creating semantic chunks...")
    
    # First, group by parent requirement
    req_groups = group_crus_by_requirement(crus)
    
    chunks = []
    chunk_counter = 1
    
    for parent_req_id, req_crus in sorted(req_groups.items()):
        # If requirement has <= max_crus_per_chunk, make it a single chunk
        if len(req_crus) <= CONFIG["max_crus_per_chunk"]:
            chunk = create_chunk_from_crus(
                req_crus, 
                chunk_id=f"CHUNK_{chunk_counter:02d}",
                parent_req_id=parent_req_id,
                application_domain=application_domain
            )
            chunks.append(chunk)
            chunk_counter += 1
        else:
            # Split large requirement into multiple chunks
            # Keep CRUs with same type together
            type_groups = group_crus_by_type(req_crus)
            
            for cru_type, type_crus in sorted(type_groups.items()):
                # Split into chunks of max size
                for i in range(0, len(type_crus), CONFIG["max_crus_per_chunk"]):
                    chunk_crus = type_crus[i:i + CONFIG["max_crus_per_chunk"]]
                    
                    # Only create chunk if meets minimum size (unless it's the only CRU)
                    if len(chunk_crus) >= CONFIG["min_crus_per_chunk"] or len(type_crus) == 1:
                        chunk = create_chunk_from_crus(
                            chunk_crus,
                            chunk_id=f"CHUNK_{chunk_counter:02d}",
                            parent_req_id=parent_req_id,
                            application_domain=application_domain
                        )
                        chunks.append(chunk)
                        chunk_counter += 1
    
    print(f"✅ Created {len(chunks)} chunks")
    return chunks


def create_chunk_from_crus(
    crus: List[Dict[str, Any]], 
    chunk_id: str,
    parent_req_id: str,
    application_domain: str
) -> Dict[str, Any]:
    """Create a single chunk from a list of CRUs."""
    
    # Determine chunk type
    cru_types = [cru.get("type", "other") for cru in crus]
    type_counts = defaultdict(int)
    for t in cru_types:
        type_counts[t] += 1
    
    # If all same type, use that; otherwise "mixed"
    if len(type_counts) == 1:
        chunk_type = list(type_counts.keys())[0]
    else:
        chunk_type = "mixed"
    
    # Extract CRU IDs
    cru_ids = [cru.get("cru_id") for cru in crus]
    
    # Create CRU payload (lightweight, essential fields only)
    cru_payload = []
    for cru in crus:
        payload_cru = {
            "cru_id": cru.get("cru_id"),
            "actor": cru.get("actor"),
            "action": cru.get("action"),
            "constraint": cru.get("constraint"),
            "confidence": cru.get("confidence")
        }
        cru_payload.append(payload_cru)
    
    # Generate capability tags (NOT domain tags)
    capability_tags = generate_capability_tags(crus)
    
    # Extract traceability
    source_requirements = list(set(cru.get("parent_requirement_id") for cru in crus))
    sections = list(set(
        cru.get("traceability", {}).get("section") 
        for cru in crus 
        if cru.get("traceability", {}).get("section")
    ))
    
    chunk = {
        "chunk_id": chunk_id,
        "chunk_type": chunk_type,
        "application_domain": [application_domain],  # Single domain per project
        "capability_tags": capability_tags,
        "cru_ids": cru_ids,
        "crus": cru_payload,  # Renamed from cru_payload for clarity
        "traceability": {
            "source_requirements": source_requirements,
            "sections": sections
        }
    }
    
    return chunk


# ======================
# Capability Tagging
# ======================

def generate_capability_tags(crus: List[Dict[str, Any]]) -> List[str]:
    """Generate capability tags from CRU actions and types.
    
    Tags are CAPABILITY/QUALITY LABELS derived from CRU text.
    NOT application domains, NOT compliance, NOT business logic.
    
    Args:
        crus: List of CRUs in this chunk
        
    Returns:
        List of capability tag labels (strings only, no confidence)
    """
    # Collect actions and types from all CRUs
    actions = []
    types = []
    
    for cru in crus:
        action = cru.get("action", "").lower()
        cru_type = cru.get("type", "")
        
        if action:
            actions.append(action)
        if cru_type:
            types.append(cru_type)
    
    # Match against capability tag patterns
    tag_scores = defaultdict(float)
    
    for capability_label, patterns in CAPABILITY_TAG_PATTERNS.items():
        matches = 0
        total_actions = len(actions)
        
        for action in actions:
            for pattern in patterns:
                if pattern.lower() in action:
                    matches += 1
                    break
        
        if matches > 0 and total_actions > 0:
            confidence = matches / total_actions
            tag_scores[capability_label] = confidence
    
    # Also tag based on CRU types
    if types:
        # Map CRU type to capability label
        type_to_label = {
            "performance": "Performance",
            "security": "Security",
            "reliability": "Reliability",
            "portability": "Portability",
            "usability": "User Interface"
        }
        
        for cru_type in set(types):
            if cru_type in type_to_label:
                label = type_to_label[cru_type]
                # Boost confidence if type-based
                tag_scores[label] = max(tag_scores.get(label, 0), 0.8)
    
    # Convert to list of labels (strings only, no confidence in output)
    capability_tags = []
    for label, confidence in sorted(tag_scores.items(), key=lambda x: x[1], reverse=True):
        if confidence >= 0.5:  # Minimum confidence threshold
            capability_tags.append(label)
    
    # If no confident tags, use "Generic"
    if not capability_tags:
        capability_tags.append("Generic")
    
    return capability_tags


# ======================
# Validation
# ======================

def validate_chunks(chunks: List[Dict[str, Any]], total_crus: int, application_domain: str) -> Dict[str, Any]:
    """Validate chunk output meets requirements.
    
    Returns: Validation report
    """
    all_cru_ids = set()
    max_size = 0
    min_size = float('inf')
    
    # Check that all chunks have the same application domain
    domain_consistency = all(
        chunk.get("application_domain", [None])[0] == application_domain
        for chunk in chunks
    )
    
    for chunk in chunks:
        cru_ids = chunk.get("cru_ids", [])
        all_cru_ids.update(cru_ids)
        
        chunk_size = len(cru_ids)
        max_size = max(max_size, chunk_size)
        min_size = min(min_size, chunk_size)
    
    report = {
        "total_chunks": len(chunks),
        "total_cru_ids_in_chunks": len(all_cru_ids),
        "expected_crus": total_crus,
        "all_crus_present": len(all_cru_ids) == total_crus,
        "max_chunk_size": max_size,
        "min_chunk_size": min_size,
        "size_limit_violated": max_size > CONFIG["max_crus_per_chunk"],
        "domain_consistency": domain_consistency,
        "application_domain": application_domain
    }
    
    return report


# ======================
# Main Pipeline
# ======================

def chunk_and_tag_crus(
    cru_json_path: str,
    output_path: str,
    application_domain: str = DEFAULT_APPLICATION_DOMAIN
) -> Dict[str, Any]:
    """Main pipeline: Load CRUs, chunk them, tag with capability labels.
    
    Args:
        cru_json_path: Path to cru_units.json
        output_path: Path to save chunked output
        application_domain: Application domain from fixed vocabulary
        
    Returns:
        Output dictionary
    """
    print("="*70)
    print("🚀 STAGE 4: SEMANTIC CHUNKING & CAPABILITY TAGGING")
    print("="*70)
    
    # Validate application domain
    if application_domain not in APPLICATION_DOMAINS:
        print(f"⚠️  WARNING: '{application_domain}' not in APPLICATION_DOMAINS")
        print(f"    Available domains: {', '.join(APPLICATION_DOMAINS)}")
        print(f"    Proceeding with provided domain...")
    
    print(f"\n🏷️  Application Domain: {application_domain}")
    
    # Load CRUs
    print(f"\n📂 Loading CRUs from: {cru_json_path}")
    with open(cru_json_path, 'r', encoding='utf-8') as f:
        cru_data = json.load(f)
    
    crus = cru_data.get("crus", [])
    print(f"✅ Loaded {len(crus)} CRUs")
    
    # Create chunks
    chunks = create_chunks(crus, application_domain)
    
    # Validate
    print("\n🔍 Validating chunks...")
    validation_report = validate_chunks(chunks, len(crus), application_domain)
    
    if not validation_report["all_crus_present"]:
        print(f"⚠️  WARNING: Not all CRUs present in chunks!")
        print(f"   Expected: {validation_report['expected_crus']}")
        print(f"   Found: {validation_report['total_cru_ids_in_chunks']}")
    
    if validation_report["size_limit_violated"]:
        print(f"⚠️  WARNING: Chunk size limit violated!")
        print(f"   Max allowed: {CONFIG['max_crus_per_chunk']}")
        print(f"   Found: {validation_report['max_chunk_size']}")
    
    if not validation_report["domain_consistency"]:
        print(f"⚠️  WARNING: Inconsistent application domains across chunks!")
    
    print("✅ Validation complete")
    
    # Calculate statistics
    avg_chunk_size = sum(len(c["cru_ids"]) for c in chunks) / len(chunks) if chunks else 0
    
    # Count capability tag distribution
    capability_tag_counts = defaultdict(int)
    for chunk in chunks:
        for tag in chunk.get("capability_tags", []):
            capability_tag_counts[tag] += 1
    
    # Prepare output
    output = {
        "metadata": {
            "total_crus": len(crus),
            "total_chunks": len(chunks),
            "avg_chunk_size": round(avg_chunk_size, 2),
            "max_chunk_size": validation_report["max_chunk_size"],
            "min_chunk_size": validation_report["min_chunk_size"],
            "application_domain": application_domain,
            "capability_tag_distribution": dict(capability_tag_counts),
            "stage": "Stage 4: Semantic Chunking & Capability Tagging",
            "version": "2.0"
        },
        "chunks": chunks
    }
    
    # Save output
    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Saved output to: {output_path}")
    
    return output


# ======================
# CLI Entry Point
# ======================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Stage 4: Chunk CRUs and tag with capability labels")
    parser.add_argument("--input", required=True, help="Input CRU units JSON file")
    parser.add_argument("--output", required=True, help="Output chunked JSON file")
    parser.add_argument("--domain", default=DEFAULT_APPLICATION_DOMAIN, 
                       help=f"Application domain (default: {DEFAULT_APPLICATION_DOMAIN})")
    args = parser.parse_args()
    
    # Run pipeline
    result = chunk_and_tag_crus(args.input, args.output, args.domain)
    
    # Display summary
    print(f"\n{'='*70}")
    print("📊 SUMMARY")
    print(f"{'='*70}")
    print(f"Application Domain:   {result['metadata']['application_domain']}")
    print(f"Total CRUs:           {result['metadata']['total_crus']}")
    print(f"Total Chunks:         {result['metadata']['total_chunks']}")
    print(f"Avg Chunk Size:       {result['metadata']['avg_chunk_size']}")
    print(f"Max Chunk Size:       {result['metadata']['max_chunk_size']}")
    print(f"Min Chunk Size:       {result['metadata']['min_chunk_size']}")
    
    print(f"\nCapability Tag Distribution:")
    for label, count in sorted(result['metadata']['capability_tag_distribution'].items(), 
                                key=lambda x: x[1], reverse=True):
        print(f"  {label:20s}  {count}")
    
    # Show sample chunk
    if result['chunks']:
        print(f"\n🔹 Sample Chunk:")
        sample = result['chunks'][0]
        print(f"  Chunk ID:           {sample['chunk_id']}")
        print(f"  Chunk Type:         {sample['chunk_type']}")
        print(f"  Application Domain: {', '.join(sample['application_domain'])}")
        print(f"  Capability Tags:    {', '.join(sample['capability_tags'])}")
        print(f"  CRU Count:          {len(sample['cru_ids'])}")
        print(f"  CRU IDs:            {', '.join(sample['cru_ids'])}")
        print(f"  Requirements:       {', '.join(sample['traceability']['source_requirements'])}")
    
    print(f"{'='*70}")
    print("✅ STAGE 4 COMPLETE")
    print(f"{'='*70}")