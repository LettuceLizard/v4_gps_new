#!/usr/bin/env python3
"""
GPS Base Station Alignment Test Suite
Tests for validating the GPS coordinate offset implementation.
"""

import json
import numpy as np
from pathlib import Path
import sys
import tempfile
import os

# Add the current directory to path to import our modules
sys.path.insert(0, '.')

from start_data_collection import (
    UM980Reader,
    apply_base_station_offset,
    convert_gps_to_enu_cm,
    load_calibration_offset,
    store_base_station_metadata
)

def create_test_gps_data(lat, lon, height, pos_type="NARROW_INT"):
    """Create a test GPSData object"""
    gps_data = UM980Reader.GPSData()
    gps_data.lat = lat
    gps_data.lon = lon
    gps_data.height = height
    gps_data.pos_type = pos_type
    gps_data.undulation = 0.0
    return gps_data

def test_zero_offset_validation():
    """Test that zero offset produces identical coordinates"""
    print("🧪 Test 1: Zero Offset Validation")
    print("=" * 50)

    # Create test base station coordinates (typical GPS coordinates)
    original_coords = create_test_gps_data(
        lat=37.7749,     # San Francisco latitude
        lon=-122.4194,   # San Francisco longitude
        height=100.0     # 100m altitude
    )

    # Apply zero offset
    zero_offset_coords = apply_base_station_offset(original_coords, 0.0, 'east')

    # Check that coordinates are identical
    lat_diff = abs(zero_offset_coords.lat - original_coords.lat)
    lon_diff = abs(zero_offset_coords.lon - original_coords.lon)
    height_diff = abs(zero_offset_coords.height - original_coords.height)

    print(f"📍 Original coordinates: ({original_coords.lat:.9f}, {original_coords.lon:.9f}, {original_coords.height:.3f})")
    print(f"📍 Zero offset coordinates: ({zero_offset_coords.lat:.9f}, {zero_offset_coords.lon:.9f}, {zero_offset_coords.height:.3f})")
    print(f"📏 Differences: lat={lat_diff:.2e}, lon={lon_diff:.2e}, height={height_diff:.6f}m")

    # Allow for small floating point differences (< 1mm)
    tolerance = 1e-8  # degrees (roughly 1mm at Earth's surface)
    height_tolerance = 0.001  # 1mm

    if lat_diff < tolerance and lon_diff < tolerance and height_diff < height_tolerance:
        print("✅ PASS: Zero offset produces identical coordinates")
        return True
    else:
        print("❌ FAIL: Zero offset changed coordinates beyond tolerance")
        return False

def test_physical_offset_validation():
    """Test that 30cm offset produces expected coordinate change"""
    print("\n🧪 Test 2: Physical Offset Validation (30cm)")
    print("=" * 50)

    # Create test coordinates
    base_coords = create_test_gps_data(37.7749, -122.4194, 100.0)

    # Apply 30cm offset in east direction
    offset_coords = apply_base_station_offset(base_coords, 0.30, 'east')

    # Calculate ENU difference to verify offset was applied correctly
    enu_diff = convert_gps_to_enu_cm(base_coords, offset_coords)

    print(f"📍 Original coordinates: ({base_coords.lat:.9f}, {base_coords.lon:.9f}, {base_coords.height:.3f})")
    print(f"📍 Offset coordinates: ({offset_coords.lat:.9f}, {offset_coords.lon:.9f}, {offset_coords.height:.3f})")
    print(f"📏 ENU difference: X={enu_diff['x_cm']:.1f}cm, Y={enu_diff['y_cm']:.1f}cm, Z={enu_diff['z_cm']:.1f}cm")

    # Check that the offset is approximately 30cm in the east direction
    expected_offset_cm = 30.0
    tolerance_cm = 1.0  # 1cm tolerance

    x_error = abs(enu_diff['x_cm'] - expected_offset_cm)
    y_error = abs(enu_diff['y_cm'])  # Should be near zero
    z_error = abs(enu_diff['z_cm'])  # Should be near zero

    print(f"📊 Offset errors: X={x_error:.2f}cm, Y={y_error:.2f}cm, Z={z_error:.2f}cm")

    if x_error < tolerance_cm and y_error < tolerance_cm and z_error < tolerance_cm:
        print("✅ PASS: 30cm east offset applied correctly")
        return True
    else:
        print("❌ FAIL: 30cm offset not applied correctly")
        return False

def test_direction_validation():
    """Test offset application in different directions"""
    print("\n🧪 Test 3: Direction Validation")
    print("=" * 50)

    base_coords = create_test_gps_data(37.7749, -122.4194, 100.0)
    offset_distance = 0.50  # 50cm for easier measurement

    directions = ['east', 'north', 'up']
    results = {}

    for direction in directions:
        offset_coords = apply_base_station_offset(base_coords, offset_distance, direction)
        enu_diff = convert_gps_to_enu_cm(base_coords, offset_coords)
        results[direction] = enu_diff

        print(f"📐 {direction.upper()} offset: X={enu_diff['x_cm']:.1f}cm, Y={enu_diff['y_cm']:.1f}cm, Z={enu_diff['z_cm']:.1f}cm")

    # Validate that each direction affects the correct axis
    tolerance = 2.0  # 2cm tolerance
    expected_offset_cm = 50.0

    # East should affect X axis
    east_valid = (abs(results['east']['x_cm'] - expected_offset_cm) < tolerance and
                  abs(results['east']['y_cm']) < tolerance and
                  abs(results['east']['z_cm']) < tolerance)

    # North should affect Y axis
    north_valid = (abs(results['north']['x_cm']) < tolerance and
                   abs(results['north']['y_cm'] - expected_offset_cm) < tolerance and
                   abs(results['north']['z_cm']) < tolerance)

    # Up should affect Z axis
    up_valid = (abs(results['up']['x_cm']) < tolerance and
                abs(results['up']['y_cm']) < tolerance and
                abs(results['up']['z_cm'] - expected_offset_cm) < tolerance)

    if east_valid and north_valid and up_valid:
        print("✅ PASS: All directions applied correctly")
        return True
    else:
        print("❌ FAIL: Direction validation failed")
        print(f"   East valid: {east_valid}, North valid: {north_valid}, Up valid: {up_valid}")
        return False

def test_config_system():
    """Test the configuration system"""
    print("\n🧪 Test 4: Configuration System")
    print("=" * 50)

    # Create test config
    test_config = {
        "calibration_offset_meters": 0.05,
        "physical_offset_meters": 0.30,
        "boresight_direction": "north",
        "notes": "Test configuration"
    }

    # Write test config
    with open('test_radar_alignment_config.json', 'w') as f:
        json.dump(test_config, f, indent=2)

    # Test loading with different filename (should use defaults)
    default_offset, default_direction = load_calibration_offset()
    print(f"📋 Default config: offset={default_offset:.3f}m, direction={default_direction}")

    # Rename test config to actual filename and test loading
    os.rename('test_radar_alignment_config.json', 'radar_alignment_config.json')
    loaded_offset, loaded_direction = load_calibration_offset()
    print(f"📋 Loaded config: offset={loaded_offset:.3f}m, direction={loaded_direction}")

    # Clean up
    os.remove('radar_alignment_config.json')

    # Restore original config if it existed
    if Path('radar_alignment_config.json.backup').exists():
        os.rename('radar_alignment_config.json.backup', 'radar_alignment_config.json')

    if loaded_offset == 0.05 and loaded_direction == "north":
        print("✅ PASS: Configuration system working correctly")
        return True
    else:
        print("❌ FAIL: Configuration system not working")
        return False

def test_metadata_storage():
    """Test metadata storage functionality"""
    print("\n🧪 Test 5: Metadata Storage")
    print("=" * 50)

    original_coords = create_test_gps_data(37.7749, -122.4194, 100.0)
    offset_coords = apply_base_station_offset(original_coords, 0.30, 'east')

    # Store metadata
    store_base_station_metadata(original_coords, offset_coords, 0.30, 'east')

    # Check that file was created and contains expected data
    try:
        with open('base_station_coordinates.json', 'r') as f:
            metadata = json.load(f)

        print(f"📝 Metadata saved successfully")
        print(f"   Original lat: {metadata['original_coordinates']['latitude']:.6f}")
        print(f"   Offset lat: {metadata['offset_coordinates']['latitude']:.6f}")
        print(f"   Total offset: {metadata['offset_parameters']['total_offset_meters']:.2f}m")
        print(f"   Boresight: {metadata['offset_parameters']['boresight_direction']}")

        # Clean up
        os.remove('base_station_coordinates.json')

        print("✅ PASS: Metadata storage working correctly")
        return True

    except Exception as e:
        print(f"❌ FAIL: Metadata storage failed: {e}")
        return False

def run_all_tests():
    """Run all validation tests"""
    print("🧪 GPS Base Station Alignment Test Suite")
    print("=" * 60)
    print("Testing the GPS coordinate offset implementation...")
    print()

    # Backup existing config if present
    if Path('radar_alignment_config.json').exists():
        os.rename('radar_alignment_config.json', 'radar_alignment_config.json.backup')

    try:
        tests = [
            test_zero_offset_validation,
            test_physical_offset_validation,
            test_direction_validation,
            test_config_system,
            test_metadata_storage
        ]

        passed = 0
        failed = 0

        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"❌ EXCEPTION in {test.__name__}: {e}")
                failed += 1

        print("\n" + "=" * 60)
        print("🏁 TEST SUMMARY")
        print("=" * 60)
        print(f"✅ Passed: {passed}")
        print(f"❌ Failed: {failed}")
        print(f"📊 Success rate: {passed/(passed+failed)*100:.1f}%")

        if failed == 0:
            print("\n🎉 ALL TESTS PASSED! The GPS alignment system is ready for field testing.")
        else:
            print(f"\n⚠️ {failed} TESTS FAILED. Please review the implementation before deployment.")

        return failed == 0

    finally:
        # Restore original config if it was backed up
        if Path('radar_alignment_config.json.backup').exists():
            if Path('radar_alignment_config.json').exists():
                os.remove('radar_alignment_config.json')
            os.rename('radar_alignment_config.json.backup', 'radar_alignment_config.json')

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)