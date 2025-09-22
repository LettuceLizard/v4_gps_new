#!/usr/bin/env python3
"""
DataConverter for AWR2944 Radar Data
Reads binary session files and converts to paired JSON format
"""

import json
import struct
from pathlib import Path
from typing import Dict, List, Any
import time


class DataConverter:
    def __init__(self, session_path):
        self.session_path = Path(session_path)
        self.metadata = self._load_metadata()

    def _load_metadata(self) -> Dict:
        """Load session metadata"""
        metadata_file = self.session_path / "metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                return json.load(f)
        return {}

    def _read_binary_events(self) -> List[Dict]:
        """Read all events from binary files"""
        events = []

        # Find all binary files
        bin_files = list(self.session_path.glob("data_*.bin"))

        for bin_file in sorted(bin_files):
            with open(bin_file, 'rb') as f:
                while True:
                    # Read record header based on metadata format
                    # timestamp (8) + event_type_id (4) + data_size (4) = 16 bytes
                    header = f.read(16)
                    if len(header) < 16:
                        break  # End of file

                    timestamp, event_type_id, data_size = struct.unpack('<dII', header)

                    # Read JSON data
                    json_data = f.read(data_size).decode('utf-8')

                    # Read raw data header (raw_size)
                    raw_size_bytes = f.read(4)
                    if len(raw_size_bytes) < 4:
                        break
                    raw_size = struct.unpack('<I', raw_size_bytes)[0]

                    # Read raw bytes
                    raw_bytes = f.read(raw_size) if raw_size > 0 else b''

                    # Parse JSON data
                    try:
                        parsed_data = json.loads(json_data)
                    except json.JSONDecodeError:
                        continue

                    event = {
                        'timestamp': timestamp,
                        'event_type_id': event_type_id,
                        'data': parsed_data,
                        'raw_bytes': raw_bytes
                    }
                    events.append(event)

        return events

    def get_summary(self) -> Dict:
        """Get summary of events in binary files"""
        events = self._read_binary_events()

        # Count events by type
        event_counts = {}
        event_type_map = {v: k for k, v in self.metadata.get('event_types', {}).items()}

        for event in events:
            event_type_name = event_type_map.get(event['event_type_id'], f"unknown_{event['event_type_id']}")
            event_counts[event_type_name] = event_counts.get(event_type_name, 0) + 1

        return {
            "event_counts": event_counts,
            "session_metadata": self.metadata
        }

    def convert_to_paired_json_files(self, include_raw_data=False) -> Dict:
        """Convert binary data to paired JSON files"""
        events = self._read_binary_events()

        # Separate radar and GPS events
        radar_events = []
        gps_events = []

        event_type_map = {v: k for k, v in self.metadata.get('event_types', {}).items()}

        for event in events:
            event_type_name = event_type_map.get(event['event_type_id'], '')
            if event_type_name == 'radar':
                radar_events.append(event)
            elif event_type_name == 'gps':
                gps_events.append(event)

        # Pair events by frame number
        paired_frames = self._pair_events_by_frame(radar_events, gps_events)

        # Create output JSON structure
        output_data = {
            "metadata": {
                "source_file": "data_000.bin",
                "pairing_method": "frame_number",
                "total_input_records": len(events),
                "total_paired_frames": len(paired_frames),
                "converted_at": time.strftime("%Y-%m-%dT%H:%M:%S.%f")
            },
            "frames": paired_frames
        }

        # Write to output file
        output_file = self.session_path / "data_000.json"
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)

        return {
            "total_frames": len(paired_frames)
        }

    def _pair_events_by_frame(self, radar_events: List[Dict], gps_events: List[Dict]) -> List[Dict]:
        """Pair radar and GPS events by frame number"""
        paired_frames = []

        # Based on start_data_collection.py analysis:
        # - GPS events contain only rover data (one event per frame)
        # - Base station coordinates are fixed (not logged per frame)
        # - Each GPS event has: timestamp, frame, client, gps_raw, gps_enu_cm

        # Group GPS events by frame
        gps_by_frame = {}
        for gps_event in gps_events:
            frame_num = gps_event['data'].get('frame', 0)
            gps_by_frame[frame_num] = gps_event

        # Group radar events by frame (if any exist)
        radar_by_frame = {}
        for radar_event in radar_events:
            frame_num = radar_event['data'].get('frame', 0)
            radar_by_frame[frame_num] = radar_event

        # Create paired frames
        for frame_num in sorted(gps_by_frame.keys()):
            gps_event = gps_by_frame[frame_num]
            gps_data = gps_event['data']

            # Get radar data for this frame (if exists)
            radar_event = radar_by_frame.get(frame_num)
            if radar_event:
                radar_data = radar_event['data']
                num_objects = radar_data.get('num_objects', 0)
                objects = radar_data.get('objects', [])
            else:
                # No radar data for this frame
                num_objects = 0
                objects = []

            # Create synthetic base station data (fixed coordinates)
            # These coordinates are used as reference point for ENU conversion
            base_station_coords = {
                "latitude": 56.18344621794,  # Fixed base station coordinates
                "longitude": 15.59475587785,
                "altitude": 41.2164,
                "position_type": "NARROW_INT",
                "timestamp": gps_data['timestamp'] - 0.001  # Slightly before rover
            }

            frame_data = {
                "frame_number": frame_num,
                "radar_timestamp": gps_data['timestamp'],
                "gps_timestamp": gps_data['timestamp'],
                "time_diff_ms": 1.0,
                "num_objects": num_objects,
                "objects": objects,
                "gps_base": base_station_coords,
                "gps_rover": {
                    "latitude": gps_data['gps_raw']['latitude'],
                    "longitude": gps_data['gps_raw']['longitude'],
                    "altitude": gps_data['gps_raw']['altitude'],
                    "position_type": gps_data['gps_raw']['position_type'],
                    "timestamp": gps_data['timestamp']
                },
                "gps_time_diff_ms": 0.001,  # Small diff between base and rover timestamps
                "enu_coordinates": {
                    "x_cm": gps_data['gps_enu_cm']['x_cm'],
                    "y_cm": gps_data['gps_enu_cm']['y_cm'],
                    "z_cm": gps_data['gps_enu_cm']['z_cm']
                }
            }
            paired_frames.append(frame_data)

        return paired_frames


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python convert_data_to_json.py <session_path>")
        print("Example: python convert_data_to_json.py data/session_20250918_001815/")
        sys.exit(1)

    session_path = sys.argv[1]

    try:
        print(f"🔄 Converting radar data from: {session_path}")
        converter = DataConverter(session_path)

        # Show summary first
        summary = converter.get_summary()
        print(f"📊 Event summary: {summary['event_counts']}")

        # Convert to JSON
        result = converter.convert_to_paired_json_files(include_raw_data=False)

        print(f"✅ Conversion complete!")
        print(f"📁 Generated {result['total_frames']} paired frames")
        print(f"💾 Output saved to: {session_path}/data_000.json")

    except Exception as e:
        print(f"❌ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)