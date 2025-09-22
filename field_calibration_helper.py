#!/usr/bin/env python3
"""
Field Calibration Helper for GPS Base Station Alignment
Provides tools for determining the correct calibration offset through field testing.
"""

import json
import argparse
from pathlib import Path

def update_calibration_offset(calibration_offset_meters, notes=""):
    """Update the calibration offset in the configuration file"""
    config_file = 'radar_alignment_config.json'

    try:
        # Load existing config
        with open(config_file, 'r') as f:
            config = json.load(f)

        # Update calibration offset
        old_offset = config.get('calibration_offset_meters', 0.0)
        config['calibration_offset_meters'] = calibration_offset_meters

        # Add calibration notes
        if notes:
            config['calibration_notes'] = notes

        # Update timestamp
        from datetime import datetime
        config['last_calibrated'] = datetime.now().isoformat()

        # Write back to file
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)

        total_offset = config.get('physical_offset_meters', 0.30) + calibration_offset_meters

        print(f"✅ Configuration updated successfully!")
        print(f"   Previous calibration offset: {old_offset:.3f}m")
        print(f"   New calibration offset: {calibration_offset_meters:.3f}m")
        print(f"   Physical offset: {config.get('physical_offset_meters', 0.30):.2f}m")
        print(f"   Total offset: {total_offset:.3f}m")
        print(f"   Boresight direction: {config.get('boresight_direction', 'east')}")
        if notes:
            print(f"   Notes: {notes}")

    except FileNotFoundError:
        print(f"❌ Configuration file '{config_file}' not found!")
        print("   Run the main system first to create the configuration file.")
    except Exception as e:
        print(f"❌ Error updating configuration: {e}")

def show_current_config():
    """Display the current configuration"""
    config_file = 'radar_alignment_config.json'

    try:
        with open(config_file, 'r') as f:
            config = json.load(f)

        print("📋 Current GPS Alignment Configuration:")
        print("=" * 50)
        print(f"Physical offset: {config.get('physical_offset_meters', 0.30):.2f}m")
        print(f"Calibration offset: {config.get('calibration_offset_meters', 0.0):.3f}m")
        print(f"Total offset: {config.get('physical_offset_meters', 0.30) + config.get('calibration_offset_meters', 0.0):.3f}m")
        print(f"Boresight direction: {config.get('boresight_direction', 'east')}")

        if 'last_calibrated' in config:
            print(f"Last calibrated: {config['last_calibrated']}")

        if 'calibration_notes' in config:
            print(f"Notes: {config['calibration_notes']}")

        print(f"Configuration file: {config['notes']}")

    except FileNotFoundError:
        print(f"❌ Configuration file '{config_file}' not found!")
    except Exception as e:
        print(f"❌ Error reading configuration: {e}")

def calculate_calibration_offset(measured_error_meters, measured_direction):
    """Calculate the calibration offset needed to correct a measured error"""
    direction_map = {
        'east': 1.0,
        'west': -1.0,
        'north': 1.0,  # Note: depends on coordinate system
        'south': -1.0,
        'forward': 1.0,
        'backward': -1.0
    }

    multiplier = direction_map.get(measured_direction.lower(), 1.0)
    calibration_offset = measured_error_meters * multiplier

    print(f"📐 Calibration Calculation:")
    print(f"   Measured error: {measured_error_meters:.3f}m {measured_direction}")
    print(f"   Suggested calibration offset: {calibration_offset:.3f}m")
    print(f"   This will move the base station reference {abs(calibration_offset):.3f}m {'forward' if calibration_offset > 0 else 'backward'}")

    return calibration_offset

def reset_calibration():
    """Reset calibration offset to zero"""
    update_calibration_offset(0.0, "Reset to zero for recalibration")
    print("🔄 Calibration offset reset to zero")

def main():
    parser = argparse.ArgumentParser(description='GPS Base Station Alignment Field Calibration Helper')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Show config command
    show_parser = subparsers.add_parser('show', help='Show current configuration')

    # Update calibration command
    update_parser = subparsers.add_parser('update', help='Update calibration offset')
    update_parser.add_argument('offset', type=float, help='Calibration offset in meters')
    update_parser.add_argument('--notes', default='', help='Calibration notes')

    # Calculate offset command
    calc_parser = subparsers.add_parser('calculate', help='Calculate calibration offset from measured error')
    calc_parser.add_argument('error', type=float, help='Measured error in meters')
    calc_parser.add_argument('direction', choices=['east', 'west', 'north', 'south', 'forward', 'backward'],
                           help='Direction of the error')

    # Reset command
    reset_parser = subparsers.add_parser('reset', help='Reset calibration offset to zero')

    # Auto-calibrate command (placeholder)
    auto_parser = subparsers.add_parser('auto', help='Interactive calibration wizard')

    args = parser.parse_args()

    if args.command == 'show':
        show_current_config()
    elif args.command == 'update':
        update_calibration_offset(args.offset, args.notes)
    elif args.command == 'calculate':
        offset = calculate_calibration_offset(args.error, args.direction)
        response = input(f"\nApply this calibration offset ({offset:.3f}m)? [y/N]: ")
        if response.lower() in ['y', 'yes']:
            update_calibration_offset(offset, f"Auto-calculated from {args.error:.3f}m {args.direction} error")
    elif args.command == 'reset':
        reset_calibration()
    elif args.command == 'auto':
        print("🧭 Interactive Calibration Wizard")
        print("=" * 40)
        print("1. First, run the system with current settings")
        print("2. Place a drone at a known, surveyed position")
        print("3. Measure the difference between radar detection and GPS-reported drone position")
        print("4. Use the 'calculate' command to determine the calibration offset")
        print("5. Apply the offset and test again")
        print()
        print("Example commands:")
        print("  python field_calibration_helper.py calculate 0.15 east")
        print("  python field_calibration_helper.py update 0.15 --notes 'Field test correction'")
        print("  python field_calibration_helper.py show")
    else:
        parser.print_help()

if __name__ == "__main__":
    main()