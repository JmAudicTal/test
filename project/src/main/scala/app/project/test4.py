#!/usr/bin/env python3
# file: convert_junit_to_xray_json.py

import os
import sys
import json
import xml.etree.ElementTree as ET
import glob
import re

# Regex pattern to extract Jira key (e.g., P1234-5678)
JIRA_KEY_PATTERN = re.compile(r"([A-Z][A-Z0-9]+-\d+)")

def extract_tests(xml_path):
    tests = []
    tree = ET.parse(xml_path)
    root = tree.getroot()

    for case in root.iter("testcase"):
        name = case.attrib.get("name", "")
        match = JIRA_KEY_PATTERN.search(name)
        if not match:
            continue

        key = match.group(1)
        if case.find("failure") is not None or case.find("error") is not None:
            status = "FAILED"
        elif case.find("skipped") is not None:
            status = "SKIPPED"
        else:
            status = "PASSED"

        tests.append({
            "testKey": key,
            "status": status,
            "comment": name,
            "steps": []
        })
    return tests

def main():
    if len(sys.argv) != 3:
        print("Usage: python convert_junit_to_xray_json.py <surefire-report-dir> <output-json-file>")
        sys.exit(1)

    report_dir = sys.argv[1]
    output_file = sys.argv[2]

    all_tests = []
    for path in glob.glob(os.path.join(report_dir, "TEST-*.xml")):
        all_tests.extend(extract_tests(path))

    result = {"tests": all_tests}

    with open(output_file, "w") as f:
        json.dump(result, f, indent=2)

    print(f"✅ Xray JSON generated with {len(all_tests)} test(s): {output_file}")

if __name__ == "__main__":
    main()
