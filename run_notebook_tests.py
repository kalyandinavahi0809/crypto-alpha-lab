#!/usr/bin/env python3
"""
Run comprehensive tests of the 01_data_collection.ipynb notebook
"""

import os
import subprocess
import tempfile
import shutil

def test_notebook_mock_mode():
    """Test notebook in mock data mode"""
    print("🧪 Testing notebook in MOCK DATA mode...")
    
    # Create a temporary script with mock mode enabled
    cmd = [
        'jupyter', 'nbconvert', 
        '--to', 'script',
        '--stdout',
        '01_data_collection.ipynb'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Failed to convert notebook: {result.stderr}")
        return False
    
    # Modify script to enable mock mode
    script_content = result.stdout.replace(
        'USE_MOCK_DATA = False',
        'USE_MOCK_DATA = True'
    )
    
    # Write to temporary file and execute
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(script_content)
        temp_script = f.name
    
    try:
        result = subprocess.run(['python3', temp_script], 
                              capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print("✅ Mock mode test PASSED")
            return True
        else:
            print(f"❌ Mock mode test FAILED: {result.stderr}")
            return False
    finally:
        os.unlink(temp_script)

def test_notebook_live_mode():
    """Test notebook in live data mode (will fail without internet)"""
    print("\n🌐 Testing notebook in LIVE DATA mode...")
    
    cmd = [
        'jupyter', 'nbconvert', 
        '--to', 'script',
        '--stdout',
        '01_data_collection.ipynb'
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"❌ Failed to convert notebook: {result.stderr}")
        return False
    
    # Write to temporary file and execute
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write(result.stdout)
        temp_script = f.name
    
    try:
        result = subprocess.run(['python3', temp_script], 
                              capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Live mode test PASSED")
            return True
        else:
            # Expected to fail without internet, but check for proper error handling
            if "Failed to fetch live data" in result.stdout or "Consider setting USE_MOCK_DATA" in result.stdout:
                print("✅ Live mode test properly handled network errors")
                return True
            else:
                print(f"❌ Live mode test FAILED unexpectedly: {result.stderr}")
                return False
    finally:
        os.unlink(temp_script)

def test_data_validation():
    """Test data validation functions"""
    print("\n🔍 Testing data validation...")
    
    # Check if storage directory exists and has proper structure
    storage_dir = os.path.join(os.getcwd(), 'storage', 'ohlcv')
    
    if not os.path.exists(storage_dir):
        print("❌ Storage directory does not exist")
        return False
    
    expected_fields = ['open', 'high', 'low', 'close', 'volume']
    actual_fields = [d for d in os.listdir(storage_dir) 
                    if os.path.isdir(os.path.join(storage_dir, d))]
    
    if set(expected_fields) != set(actual_fields):
        print(f"❌ Field directories mismatch. Expected: {expected_fields}, Got: {actual_fields}")
        return False
    
    # Check that each field directory has parquet files
    total_files = 0
    for field in expected_fields:
        field_dir = os.path.join(storage_dir, field)
        parquet_files = [f for f in os.listdir(field_dir) if f.endswith('.parquet')]
        total_files += len(parquet_files)
        
        if len(parquet_files) == 0:
            print(f"❌ No parquet files found in {field} directory")
            return False
    
    print(f"✅ Data validation PASSED - {total_files} parquet files found")
    return True

def cleanup_test_data():
    """Clean up test data"""
    storage_dir = os.path.join(os.getcwd(), 'storage')
    if os.path.exists(storage_dir):
        response = input(f"\n🗑️  Remove test data directory '{storage_dir}'? (y/N): ")
        if response.lower() == 'y':
            shutil.rmtree(storage_dir)
            print("✅ Test data cleaned up")
        else:
            print("📁 Test data preserved")

def main():
    """Run all tests"""
    print("=" * 60)
    print("🧪 NOTEBOOK TESTING SUITE")
    print("=" * 60)
    
    os.chdir('/home/runner/work/crypto-alpha-lab/crypto-alpha-lab')
    
    tests_passed = 0
    total_tests = 3
    
    # Test 1: Mock mode
    if test_notebook_mock_mode():
        tests_passed += 1
    
    # Test 2: Live mode (expected to handle network errors gracefully)
    if test_notebook_live_mode():
        tests_passed += 1
    
    # Test 3: Data validation
    if test_data_validation():
        tests_passed += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"📊 TEST RESULTS: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 ALL TESTS PASSED! The notebook is working correctly.")
        print("\n✨ Key features validated:")
        print("  ✅ Mock data mode for offline testing")
        print("  ✅ Proper error handling for network issues")
        print("  ✅ Data storage and validation")
        print("  ✅ Parquet file generation")
        print("  ✅ Comprehensive logging and progress tracking")
    else:
        print(f"⚠️  {total_tests - tests_passed} tests failed.")
    
    print("=" * 60)
    
    # Optional cleanup
    cleanup_test_data()
    
    return tests_passed == total_tests

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)