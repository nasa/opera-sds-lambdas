#!/usr/bin/env python3
"""
Test script to demonstrate the strptime refactoring approach.
"""

from datetime import datetime, timezone
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from datetime_utils import parse_strptime_datetime

def test_strptime_refactoring():
    """Test that our utility function works the same as the old approach."""
    
    print("Testing strptime refactoring...")
    print("=" * 50)
    
    # Test cases with different datetime formats
    test_cases = [
        ("2023-12-01T10:30:00", "%Y-%m-%dT%H:%M:%S"),
        ("2023-12-01T10:30:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        ("20231201T103000", "%Y%m%dT%H%M%S"),
        ("01/12/2023 10:30:00", "%d/%m/%Y %H:%M:%S"),
    ]
    
    for dt_str, format_str in test_cases:
        print(f"\nTesting: {dt_str} with format {format_str}")
        
        # Old approach (manual timezone replacement)
        dt_old = datetime.strptime(dt_str, format_str).replace(tzinfo=timezone.utc)
        print(f"  Old approach: {dt_old} (tzinfo: {dt_old.tzinfo})")
        
        # New approach (utility function)
        dt_new = parse_strptime_datetime(dt_str, format_str)
        print(f"  New approach: {dt_new} (tzinfo: {dt_new.tzinfo})")
        
        # Verify they're equivalent
        assert dt_old == dt_new, f"Results should be equivalent for {dt_str}"
        assert dt_new.tzinfo is not None, f"New approach should return timezone-aware datetime for {dt_str}"
        
        print(f"  ✅ Both approaches produce equivalent timezone-aware datetimes")
    
    print("\n" + "=" * 50)
    print("✅ All tests passed! The refactoring works correctly.")
    
    # Test comparison between timezone-aware datetimes
    print("\nTesting datetime comparisons...")
    dt1 = parse_strptime_datetime("2023-12-01T10:30:00", "%Y-%m-%dT%H:%M:%S")
    dt2 = parse_strptime_datetime("2023-12-01T11:30:00", "%Y-%m-%dT%H:%M:%S")
    
    print(f"dt1: {dt1} (tzinfo: {dt1.tzinfo})")
    print(f"dt2: {dt2} (tzinfo: {dt2.tzinfo})")
    print(f"dt1 < dt2: {dt1 < dt2}")
    print(f"dt1 == dt2: {dt1 == dt2}")
    
    # This should not raise a TypeError
    try:
        comparison_result = dt1 < dt2
        print("✅ Datetime comparison successful - no TypeError!")
    except TypeError as e:
        print(f"❌ TypeError during comparison: {e}")

if __name__ == "__main__":
    test_strptime_refactoring()
