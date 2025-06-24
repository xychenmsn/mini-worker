#!/usr/bin/env python3
"""
Test summary script for mini-worker
"""

import subprocess
import sys

def run_test_group(name, test_path):
    """Run a group of tests and return results"""
    print(f"\n{'='*60}")
    print(f"Running {name}")
    print('='*60)
    
    try:
        result = subprocess.run(
            [sys.executable, '-m', 'pytest', test_path, '-v', '--tb=short'],
            capture_output=True,
            text=True,
            timeout=120
        )
        
        print(f"Return code: {result.returncode}")
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT: {name} tests took too long")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def main():
    """Run all test groups and summarize results"""
    test_groups = [
        ("Original Core Tests", "tests/test_base_worker.py tests/test_cli.py tests/test_manager.py"),
        ("Integration Scenarios", "tests/test_integration_scenarios.py"),
        ("Performance & Stress Tests", "tests/test_performance_stress.py"),
        ("CLI Integration Tests", "tests/test_cli_integration.py"),
        ("Deployment & Packaging Tests", "tests/test_deployment_packaging.py"),
    ]
    
    results = {}
    
    for name, path in test_groups:
        results[name] = run_test_group(name, path)
    
    # Summary
    print(f"\n{'='*60}")
    print("TEST SUMMARY")
    print('='*60)
    
    total_groups = len(test_groups)
    passed_groups = sum(1 for passed in results.values() if passed)
    
    for name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{name}: {status}")
    
    print(f"\nOverall: {passed_groups}/{total_groups} test groups passed")
    
    if passed_groups == total_groups:
        print("🎉 All test groups passed!")
        return 0
    else:
        print("⚠️  Some test groups failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
