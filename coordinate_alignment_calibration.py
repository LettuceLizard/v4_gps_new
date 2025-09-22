#!/usr/bin/env python3
"""
Coordinate System Alignment Calibration
Interactive calibration to determine radar boresight direction by analyzing movement data.
"""

import time
import numpy as np
from collections import deque
import json
from datetime import datetime
from start_data_collection import UM980Reader, convert_gps_to_enu_cm

def run_boresight_calibration(base_station_port, rover_port, baudrate=115200):
    """
    Determine radar boresight direction using rover-to-base GPS vector

    Process:
    1. Get base station GPS position
    2. Get rover GPS position
    3. Calculate vector from base station to rover
    4. Assume this vector represents the radar boresight direction
    5. Determine which coordinate axis (X/Y/Z) has the largest component
    6. Update configuration with detected boresight direction

    ASSUMPTION: Rover is positioned in the radar boresight direction from base station
    """

    print("🧭 RADAR BORESIGHT CALIBRATION")
    print("=" * 60)
    print("This calibration determines which coordinate axis represents")
    print("the radar boresight (forward) direction using GPS data.")
    print()
    print("METHOD:")
    print("- Uses the vector from base station to rover GPS")
    print("- Assumes rover is positioned in radar boresight direction")
    print("- Analyzes which coordinate axis has the largest component")
    print("- No physical movement required!")
    print()

    # Step 1: Get base station and rover coordinates
    print("📍 Step 1: Getting GPS coordinates...")
    base_reader = UM980Reader(base_station_port, baudrate, "BASE_CALIBRATION")
    rover_reader = UM980Reader(rover_port, baudrate, "ROVER_CALIBRATION")

    try:
        base_reader.start()
        rover_reader.start()
        print("✅ Both GPS readers started")

        # Wait for positions from both GPS units
        base_coords = None
        rover_coords = None

        print("⏳ Waiting for GPS positions...")
        for attempt in range(30):
            base_data = base_reader.get_data()
            rover_data = rover_reader.get_data()

            if base_data.lat is not None and base_data.pos_type in ['NARROW_INT', 'WIDE_INT', 'L1_FLOAT', 'IONOFREE_FLOAT', 'FIXEDPOS']:
                if base_coords is None:
                    base_coords = base_data
                    print(f"✅ Base station position: ({base_data.lat:.9f}, {base_data.lon:.9f}, {base_data.height:.3f})")
                    print(f"   Position type: {base_data.pos_type}")

            if rover_data.lat is not None and rover_data.pos_type in ['NARROW_INT', 'WIDE_INT', 'L1_FLOAT', 'IONOFREE_FLOAT', 'FIXEDPOS']:
                if rover_coords is None:
                    rover_coords = rover_data
                    print(f"✅ Rover position: ({rover_data.lat:.9f}, {rover_data.lon:.9f}, {rover_data.height:.3f})")
                    print(f"   Position type: {rover_data.pos_type}")

            if base_coords and rover_coords:
                break

            time.sleep(0.5)

        if not base_coords:
            print("❌ Failed to get base station coordinates")
            print(f"Check connection to {base_station_port}")
            return None

        if not rover_coords:
            print("❌ Failed to get rover coordinates")
            print(f"Check connection to {rover_port}")
            return None

        # Step 2: Calculate radar boresight vector
        print("\n📐 Step 2: Calculating Radar Boresight Vector")
        print("=" * 50)
        print("ASSUMPTION: Rover is positioned in the radar boresight direction")
        print("METHOD: Calculate base→rover vector to determine GPS direction of radar -Y axis")
        print("NOTE: AWR2944P radar has -Y axis as boresight (forward from antenna)")
        print()

        input("📍 Confirm rover is positioned in radar boresight direction, then press ENTER...")

        # Calculate 2D vector from base station to rover using ENU coordinates (horizontal only)
        rover_enu = convert_gps_to_enu_cm(base_coords, rover_coords)
        boresight_vector_2d = np.array([rover_enu['x_cm'], rover_enu['y_cm']])  # Only X and Y, no Z
        vector_distance = np.linalg.norm(boresight_vector_2d)

        print(f"📏 Base-to-rover vector (2D): X={boresight_vector_2d[0]:+6.1f}cm, Y={boresight_vector_2d[1]:+6.1f}cm")
        print(f"📐 Horizontal distance: {vector_distance:.1f}cm")
        print(f"📊 Altitude difference: {rover_enu['z_cm']:+6.1f}cm (ignored for boresight)")

        if vector_distance < 50:  # Less than 50cm
            print("⚠️ Warning: Very short base-to-rover distance. Consider positioning rover further for better accuracy.")

        # Step 3: Analyze vector direction
        print("\n🔍 Step 3: Analyzing Boresight Direction")
        print("=" * 40)

        # Determine dominant axis (2D only - East/North) and consider sign
        abs_vector_2d = np.abs(boresight_vector_2d)
        dominant_axis_idx = np.argmax(abs_vector_2d)
        dominant_value = boresight_vector_2d[dominant_axis_idx]  # Get actual value (with sign)

        # Map axis and direction based on sign
        if dominant_axis_idx == 0:  # X axis
            axis_name = 'X (East/West)'
            boresight_direction = 'east' if dominant_value > 0 else 'west'
        else:  # Y axis
            axis_name = 'Y (North/South)'
            boresight_direction = 'north' if dominant_value > 0 else 'south'

        print(f"🎯 Dominant vector axis: {axis_name}")
        print(f"🧭 Detected boresight direction: {boresight_direction}")
        print(f"📊 Vector components: X={boresight_vector_2d[0]:+6.1f}cm, Y={boresight_vector_2d[1]:+6.1f}cm")
        print(f"📊 Vector ratio: X={abs_vector_2d[0]/vector_distance*100:.1f}%, Y={abs_vector_2d[1]/vector_distance*100:.1f}%")

        # Validate the result
        if abs_vector_2d[dominant_axis_idx] < vector_distance * 0.7:
            print("⚠️ Warning: Vector doesn't strongly favor one axis.")
            print("   Consider repositioning rover more clearly in radar boresight direction.")

        # Step 4: Update configuration
        print("\n💾 Step 4: Update Configuration")
        print("=" * 40)

        # Load existing config
        config_file = 'radar_alignment_config.json'
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
        except FileNotFoundError:
            config = {
                "calibration_offset_meters": 0.0,
                "physical_offset_meters": 0.30,
                "boresight_direction": "east",
                "notes": "Initial configuration"
            }

        # Update boresight direction
        old_direction = config.get('boresight_direction', 'unknown')
        config['boresight_direction'] = boresight_direction
        config['calibration_timestamp'] = datetime.now().isoformat()
        config['calibration_data'] = {
            'vector_distance_cm': float(vector_distance),
            'boresight_vector_2d_cm': boresight_vector_2d.tolist(),
            'vector_ratio_2d': (abs_vector_2d / vector_distance * 100).tolist(),
            'altitude_difference_cm': float(rover_enu['z_cm']),
            'base_coordinates': [base_coords.lat, base_coords.lon, base_coords.height],
            'rover_coordinates': [rover_coords.lat, rover_coords.lon, rover_coords.height]
        }

        # Save updated config
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)

        print(f"✅ Configuration updated!")
        print(f"   Previous boresight direction: {old_direction}")
        print(f"   New boresight direction: {boresight_direction}")
        print(f"   Configuration saved to: {config_file}")

        # Save detailed calibration data
        calibration_data = {
            'timestamp': datetime.now().isoformat(),
            'method': 'base_to_rover_vector',
            'base_coordinates': {
                'lat': base_coords.lat,
                'lon': base_coords.lon,
                'height': base_coords.height,
                'pos_type': base_coords.pos_type
            },
            'rover_coordinates': {
                'lat': rover_coords.lat,
                'lon': rover_coords.lon,
                'height': rover_coords.height,
                'pos_type': rover_coords.pos_type
            },
            'boresight_vector_2d_cm': boresight_vector_2d.tolist(),
            'vector_distance_cm': float(vector_distance),
            'altitude_difference_cm': float(rover_enu['z_cm']),
            'detected_boresight': boresight_direction,
            'axis_analysis': {
                'x_component_cm': float(boresight_vector_2d[0]),
                'y_component_cm': float(boresight_vector_2d[1]),
                'z_component_cm': float(rover_enu['z_cm']),
                'x_percentage': float(abs_vector_2d[0] / vector_distance * 100),
                'y_percentage': float(abs_vector_2d[1] / vector_distance * 100),
                'note': 'Z component ignored for 2D boresight analysis'
            }
        }

        with open('boresight_calibration_data.json', 'w') as f:
            json.dump(calibration_data, f, indent=2)

        print(f"📊 Detailed calibration data saved to: boresight_calibration_data.json")

        return boresight_direction

    except KeyboardInterrupt:
        print("\n🛑 Calibration interrupted")
        return None
    except Exception as e:
        print(f"\n❌ Calibration error: {e}")
        return None
    finally:
        base_reader.stop()
        rover_reader.stop()

def main():
    """Main calibration function"""
    import argparse

    parser = argparse.ArgumentParser(description='Radar Boresight Direction Calibration')
    parser.add_argument('--base-port', default='/dev/ttyUSB1', help='Base station GPS port (default: /dev/ttyUSB1)')
    parser.add_argument('--rover-port', default='/dev/ttyUSB0', help='Rover GPS port (default: /dev/ttyUSB0)')
    parser.add_argument('--base-baud', type=int, default=115200, help='GPS baudrate (default: 115200)')

    args = parser.parse_args()

    print("🎯 Starting Radar Boresight Calibration")
    print("This will determine which coordinate axis represents the radar's forward direction")
    print("by analyzing the vector from base station to rover GPS")
    print()

    result = run_boresight_calibration(args.base_port, args.rover_port, args.base_baud)

    if result:
        print(f"\n🎉 Calibration Complete!")
        print(f"Radar boresight direction: {result}")
        print("\nNext steps:")
        print("1. Run the main system to test alignment")
        print("2. Use field_calibration_helper.py to fine-tune if needed")
    else:
        print(f"\n❌ Calibration failed. Please try again.")

if __name__ == "__main__":
    main()