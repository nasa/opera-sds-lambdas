#!/usr/bin/env python3
"""
Comprehensive test script to verify all datetime fixes work correctly.
Tests fromisoformat, strptime, and dateutil.parser fixes.
"""

from datetime import datetime, timezone
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from datetime_utils import (
    parse_fromisoformat_datetime, 
    parse_strptime_datetime, 
    parse_iso_datetime,
    ensure_timezone_aware
)

def test_comprehensive_datetime_fixes():
    """Test all datetime utility functions to ensure they work correctly."""
    
    print("Testing comprehensive datetime fixes...")
    print("=" * 60)
    
    # Test 1: datetime.fromisoformat() replacement
    print("\n1. Testing parse_fromisoformat_datetime()...")
    test_cases_fromiso = [
        "2023-12-01T10:30:00",           # No timezone
        "2023-12-01T10:30:00Z",          # UTC timezone
        "2023-12-01T10:30:00+00:00",     # UTC timezone with offset
        "2023-12-01T10:30:00.123456",    # With microseconds
    ]
    
    for dt_str in test_cases_fromiso:
        print(f"\n  Testing: {dt_str}")
        
        # Old approach (creates timezone-naive)
        try:
            dt_old = datetime.fromisoformat(dt_str)
            print(f"    Old approach: {dt_old} (tzinfo: {dt_old.tzinfo})")
        except ValueError as e:
            print(f"    Old approach: ValueError - {e}")
            dt_old = None
        
        # New approach (always timezone-aware)
        dt_new = parse_fromisoformat_datetime(dt_str)
        print(f"    New approach: {dt_new} (tzinfo: {dt_new.tzinfo})")
        
        # Verify new approach is timezone-aware
        assert dt_new.tzinfo is not None, f"New approach should return timezone-aware datetime for {dt_str}"
        print(f"    ✅ New approach returns timezone-aware datetime")
    
    # Test 2: datetime.strptime() replacement
    print("\n\n2. Testing parse_strptime_datetime()...")
    test_cases_strptime = [
        ("2023-12-01T10:30:00", "%Y-%m-%dT%H:%M:%S"),
        ("2023-12-01T10:30:00Z", "%Y-%m-%dT%H:%M:%SZ"),
        ("20231201T103000", "%Y%m%dT%H%M%S"),
        ("01/12/2023 10:30:00", "%d/%m/%Y %H:%M:%S"),
    ]
    
    for dt_str, format_str in test_cases_strptime:
        print(f"\n  Testing: {dt_str} with format {format_str}")
        
        # Old approach (manual timezone replacement)
        dt_old = datetime.strptime(dt_str, format_str).replace(tzinfo=timezone.utc)
        print(f"    Old approach: {dt_old} (tzinfo: {dt_old.tzinfo})")
        
        # New approach (utility function)
        dt_new = parse_strptime_datetime(dt_str, format_str)
        print(f"    New approach: {dt_new} (tzinfo: {dt_new.tzinfo})")
        
        # Verify they're equivalent
        assert dt_old == dt_new, f"Results should be equivalent for {dt_str}"
        assert dt_new.tzinfo is not None, f"New approach should return timezone-aware datetime for {dt_str}"
        print(f"    ✅ Both approaches produce equivalent timezone-aware datetimes")
    
    # Test 3: dateutil.parser replacement
    print("\n\n3. Testing parse_iso_datetime()...")
    test_cases_dateutil = [
        "2023-12-01T10:30:00",           # No timezone
        "2023-12-01T10:30:00Z",          # UTC timezone
        "2023-12-01T10:30:00+00:00",     # UTC timezone with offset
        "2023-12-01T10:30:00.123456Z",   # With microseconds and timezone
    ]
    
    for dt_str in test_cases_dateutil:
        print(f"\n  Testing: {dt_str}")
        
        # Old approach (manual timezone replacement)
        import dateutil.parser
        dt_old = dateutil.parser.isoparse(dt_str).replace(tzinfo=timezone.utc)
        print(f"    Old approach: {dt_old} (tzinfo: {dt_old.tzinfo})")
        
        # New approach (smart timezone detection)
        dt_new = parse_iso_datetime(dt_str)
        print(f"    New approach: {dt_new} (tzinfo: {dt_new.tzinfo})")
        
        # Verify new approach is timezone-aware
        assert dt_new.tzinfo is not None, f"New approach should return timezone-aware datetime for {dt_str}"
        print(f"    ✅ New approach returns timezone-aware datetime")
    
    # Test 4: Cross-comparison between different datetime sources
    print("\n\n4. Testing cross-comparison between different datetime sources...")
    
    # Create datetimes from different sources
    dt_fromiso = parse_fromisoformat_datetime("2023-12-01T10:30:00")
    dt_strptime = parse_strptime_datetime("2023-12-01T10:30:00", "%Y-%m-%dT%H:%M:%S")
    dt_dateutil = parse_iso_datetime("2023-12-01T10:30:00")
    dt_now = datetime.now(timezone.utc)
    
    print(f"    dt_fromiso:  {dt_fromiso} (tzinfo: {dt_fromiso.tzinfo})")
    print(f"    dt_strptime: {dt_strptime} (tzinfo: {dt_strptime.tzinfo})")
    print(f"    dt_dateutil: {dt_dateutil} (tzinfo: {dt_dateutil.tzinfo})")
    print(f"    dt_now:      {dt_now} (tzinfo: {dt_now.tzinfo})")
    
    # Test comparisons (should not raise TypeError)
    try:
        comparison1 = dt_fromiso == dt_strptime
        comparison2 = dt_strptime == dt_dateutil
        comparison3 = dt_fromiso < dt_now
        comparison4 = dt_dateutil < dt_now
        
        print(f"    dt_fromiso == dt_strptime: {comparison1}")
        print(f"    dt_strptime == dt_dateutil: {comparison2}")
        print(f"    dt_fromiso < dt_now: {comparison3}")
        print(f"    dt_dateutil < dt_now: {comparison4}")
        
        print("    ✅ All datetime comparisons successful - no TypeError!")
        
    except TypeError as e:
        print(f"    ❌ TypeError during comparison: {e}")
        raise
    
    print("\n" + "=" * 60)
    print("✅ All comprehensive datetime tests passed!")
    print("🎉 No more timezone comparison errors possible!")

if __name__ == "__main__":
    test_comprehensive_datetime_fixes()
