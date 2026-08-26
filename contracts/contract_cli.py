#!/usr/bin/env python3
"""
Data Contract Enforcement CLI Engine (lakehouse-contract-cli)
Validates incoming raw datasets against YAML Data Contracts.
Handles contract violations, quarantines bad batches, and alerts via Webhook.
"""

import os
import sys
import json
import logging
import argparse
import urllib.request
from datetime import datetime
from pathlib import Path
import csv

# PyYAML fallback loader if PyYAML is not installed
def load_yaml(yaml_path):
    try:
        import yaml
        with open(yaml_path, 'r') as f:
            return yaml.safe_load(f)
    except ImportError:
        # Fallback simple parser for contract YAML files
        contract = {"columns": {}, "sla": {}}
        current_col = None
        with open(yaml_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if line.startswith('dataset_name:'):
                    contract['dataset_name'] = line.split(':', 1)[1].strip().strip('"')
                elif line.startswith('version:'):
                    contract['version'] = line.split(':', 1)[1].strip().strip('"')
                elif line.endswith(':') and not line.startswith('sla:') and not line.startswith('columns:'):
                    current_col = line[:-1].strip()
                    contract['columns'][current_col] = {'allowed_values': []}
                elif current_col and ':' in line:
                    k, v = line.split(':', 1)
                    k, v = k.strip(), v.strip().strip('"')
                    if k == 'required':
                        contract['columns'][current_col]['required'] = (v.lower() == 'true')
                    elif k == 'nullable':
                        contract['columns'][current_col]['nullable'] = (v.lower() == 'true')
                    elif k == 'type':
                        contract['columns'][current_col]['type'] = v
                    elif k == 'min_value':
                        contract['columns'][current_col]['min_value'] = float(v)
                elif current_col and line.startswith('- '):
                    val = line[2:].strip().strip('"')
                    contract['columns'][current_col]['allowed_values'].append(val)
        return contract


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "event": getattr(record, "event", record.msg),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        if hasattr(record, "extra"):
            log_entry.update(record.extra)
        return json.dumps(log_entry)


def setup_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    logger = logging.getLogger("contract_cli")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


def parse_args():
    parser = argparse.ArgumentParser(description="Data Contract Enforcement CLI Engine")
    parser.add_argument("--input-file", required=True, help="Path to input CSV or JSON data file")
    parser.add_argument("--contract-file", required=True, help="Path to YAML contract specification")
    parser.add_argument("--quarantine-dir", default="./data/quarantine", help="Quarantine directory for bad batches")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def validate_contract(data_path, contract):
    violations = []
    columns_spec = contract.get("columns", {})
    
    with open(data_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        actual_headers = reader.fieldnames or []
        
        # 1. Required Columns Check
        for col_name, spec in columns_spec.items():
            if spec.get("required", False) and col_name not in actual_headers:
                violations.append({
                    "rule": "missing_required_column",
                    "column": col_name,
                    "expected": f"Column '{col_name}' present",
                    "actual": "Column missing from dataset headers"
                })
        
        # Row-level check (sample up to 500 rows for performance)
        row_count = 0
        null_counts = {col: 0 for col in columns_spec}
        
        for row in reader:
            row_count += 1
            for col_name, spec in columns_spec.items():
                val = row.get(col_name)
                
                # Nullability check
                if not spec.get("nullable", True) and (val is None or val.strip() == ""):
                    null_counts[col_name] += 1
                
                # Allowed values enum check
                allowed = spec.get("allowed_values")
                if allowed and val and val.strip() not in allowed:
                    if len(violations) < 10:  # Cap detailed violations report size
                        violations.append({
                            "rule": "enum_violation",
                            "column": col_name,
                            "expected": f"One of {allowed}",
                            "actual": val,
                            "row": row_count
                        })
                
                # Min value check
                min_val = spec.get("min_value")
                if min_val is not None and val and val.strip():
                    try:
                        num_val = float(val)
                        if num_val < min_val:
                            violations.append({
                                "rule": "min_value_violation",
                                "column": col_name,
                                "expected": f">= {min_val}",
                                "actual": num_val,
                                "row": row_count
                            })
                    except ValueError:
                        pass

        # Check null rate violation threshold
        for col_name, spec in columns_spec.items():
            if not spec.get("nullable", True) and row_count > 0:
                null_rate = (null_counts[col_name] / row_count) * 100
                if null_rate > 0:
                    violations.append({
                        "rule": "nullability_violation",
                        "column": col_name,
                        "expected": "0% nulls",
                        "actual": f"{null_rate:.2f}% nulls ({null_counts[col_name]}/{row_count} rows)"
                    })

    return {
        "valid": len(violations) == 0,
        "violations": violations,
        "rows_scanned": row_count,
        "actual_headers": actual_headers
    }


def quarantine_file(input_file, quarantine_dir, logger):
    Path(quarantine_dir).mkdir(parents=True, exist_ok=True)
    file_name = Path(input_file).name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    quarantine_path = Path(quarantine_dir) / f"quarantined_{timestamp}_{file_name}"
    
    with open(input_file, 'rb') as src, open(quarantine_path, 'wb') as dst:
        dst.write(src.read())
        
    logger.warning(
        "File quarantined due to contract violation",
        extra={"event": "file_quarantined", "quarantine_path": str(quarantine_path)}
    )
    return str(quarantine_path)


def dispatch_contract_alert(contract, result, quarantine_path):
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url or not webhook_url.strip() or webhook_url.startswith("<"):
        return
    
    message = (
        f"🚨 *DATA CONTRACT VIOLATION DETECTED*\n"
        f"• *Dataset*: `{contract.get('dataset_name', 'unknown')}` (v{contract.get('version', '1.0.0')})\n"
        f"• *Status*: ❌ REJECTED & QUARANTINED\n"
        f"• *Quarantine Location*: `{quarantine_path}`\n"
        f"• *Total Violations*: `{len(result['violations'])}`\n"
        f"• *Sample Violation*: ```{json.dumps(result['violations'][:2], indent=2)}```"
    )
    
    try:
        payload = json.dumps({"text": message}).encode('utf-8')
        req = urllib.request.Request(webhook_url, data=payload, headers={'Content-Type': 'application/json'})
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        print(f"Contract alert dispatch warning: {e}")


def main():
    args = parse_args()
    logger = setup_logging()
    logger.setLevel(getattr(logging, args.log_level))
    
    logger.info("Loading Data Contract", extra={"event": "contract_load", "contract_file": args.contract_file})
    contract = load_yaml(args.contract_file)
    
    logger.info("Validating dataset against contract", extra={"event": "contract_validation_start", "input_file": args.input_file})
    result = validate_contract(args.input_file, contract)
    
    if result["valid"]:
        logger.info(
            "Data contract validation PASSED",
            extra={"event": "contract_passed", "rows_scanned": result["rows_scanned"]}
        )
        print("\n[SUCCESS] DATA CONTRACT VALIDATION PASSED")
        sys.exit(0)
    else:
        logger.error(
            "Data contract validation FAILED",
            extra={
                "event": "contract_failed",
                "violation_count": len(result["violations"]),
                "violations": result["violations"][:5]
            }
        )
        
        quarantine_path = quarantine_file(args.input_file, args.quarantine_dir, logger)
        dispatch_contract_alert(contract, result, quarantine_path)
        
        report_path = Path("logs") / f"contract_violation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        Path("logs").mkdir(exist_ok=True)
        with open(report_path, "w") as f:
            json.dump({
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "contract": contract.get("dataset_name"),
                "version": contract.get("version"),
                "quarantine_path": quarantine_path,
                "result": result
            }, f, indent=2)
            
        print(f"\n[FAILED] DATA CONTRACT VALIDATION FAILED ({len(result['violations'])} violations)")
        print(f"Quarantined File: {quarantine_path}")
        print(f"Violation Report: {report_path}")
        sys.exit(1)


if __name__ == "__main__":
    main()
