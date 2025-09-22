#!/usr/bin/env python3
"""
Post-processing script for AWR2944 radar and GPS data
Converts raw data_000.json to post_processed_data_000.json for 3D visualization
"""

import json
import numpy as np
from pathlib import Path
import sys
from typing import Dict, List, Any

class RadarDataPostProcessor:
    def __init__(self, session_path: Path):
        self.session_path = Path(session_path)
        self.input_file = self.session_path / "data_000.json"
        self.output_file = self.session_path / "post_processed_data_000.json"

        if not self.input_file.exists():
            raise FileNotFoundError(f"No data_000.json found in {self.session_path}")

    def load_data(self) -> Dict:
        """Load the raw JSON data"""
        with open(self.input_file, 'r') as f:
            return json.load(f)

    def extract_radar_objects(self, frame_data: Dict) -> List[Dict[str, float]]:
        """Extract radar object positions from frame"""
        objects = []
        for obj in frame_data.get('objects', []):
            # Radar positions are already in meters
            objects.append({
                'x': float(obj['x']),
                'y': float(obj['y']),
                'z': float(obj['z']),
                'velocity': float(obj.get('velocity', 0.0)),
                'range_idx': obj.get('range_idx'),
                'doppler_idx': obj.get('doppler_idx')
            })
        return objects

    def extract_gps_position(self, frame_data: Dict) -> Dict[str, float]:
        """Extract and convert GPS position from ENU coordinates"""
        enu = frame_data.get('enu_coordinates', {})

        # Convert from centimeters to meters
        return {
            'x': float(enu.get('x_cm', 0.0)) / 100.0,
            'y': float(enu.get('y_cm', 0.0)) / 100.0,
            'z': float(enu.get('z_cm', 0.0)) / 100.0
        }


    def process(self) -> Dict:
        """Main processing function - preserves all original data and adds processed fields"""
        print(f"Loading data from: {self.input_file}")
        raw_data = self.load_data()

        # Start with the complete original data
        output_data = raw_data.copy()

        # Add post-processing metadata alongside original metadata
        output_data['metadata']['post_processing'] = {
            'version': '1.0',
            'coordinate_system': 'ENU (East-North-Up)',
            'processed_units': 'meters',
            'origin': 'Common origin at (0,0,0) for radar and GPS'
        }

        # Process each frame while preserving all original data
        for frame in output_data['frames']:
            # Add processed data as new fields, not replacements
            frame['processed'] = {
                'radar_objects_meters': self.extract_radar_objects(frame),
                'gps_position_meters': self.extract_gps_position(frame),
                'timestamp': frame.get('radar_timestamp', frame.get('gps_timestamp'))
            }

        # Calculate statistics using processed data
        stats = self.calculate_statistics_from_enhanced_frames(output_data['frames'])
        output_data['metadata']['post_processing']['statistics'] = stats

        return output_data

    def calculate_statistics_from_enhanced_frames(self, frames: List[Dict]) -> Dict:
        """Calculate statistics from enhanced frames that contain both original and processed data"""
        all_radar_positions = []
        all_gps_positions = []
        object_counts = []

        for frame in frames:
            processed = frame.get('processed', {})
            object_counts.append(len(processed.get('radar_objects_meters', [])))

            for obj in processed.get('radar_objects_meters', []):
                all_radar_positions.append([obj['x'], obj['y'], obj['z']])

            gps = processed.get('gps_position_meters', {})
            if gps:
                all_gps_positions.append([gps['x'], gps['y'], gps['z']])

        stats = {
            'total_frames': len(frames),
            'total_radar_detections': sum(object_counts),
            'avg_objects_per_frame': np.mean(object_counts) if object_counts else 0,
            'max_objects_in_frame': max(object_counts) if object_counts else 0
        }

        if all_radar_positions:
            radar_array = np.array(all_radar_positions)
            stats['radar_bounds'] = {
                'min': {'x': float(radar_array[:, 0].min()),
                       'y': float(radar_array[:, 1].min()),
                       'z': float(radar_array[:, 2].min())},
                'max': {'x': float(radar_array[:, 0].max()),
                       'y': float(radar_array[:, 1].max()),
                       'z': float(radar_array[:, 2].max())}
            }

        if all_gps_positions:
            gps_array = np.array(all_gps_positions)
            stats['gps_bounds'] = {
                'min': {'x': float(gps_array[:, 0].min()),
                       'y': float(gps_array[:, 1].min()),
                       'z': float(gps_array[:, 2].min())},
                'max': {'x': float(gps_array[:, 0].max()),
                       'y': float(gps_array[:, 1].max()),
                       'z': float(gps_array[:, 2].max())}
            }

            # Calculate GPS path length
            if len(gps_array) > 1:
                diffs = np.diff(gps_array, axis=0)
                distances = np.sqrt(np.sum(diffs**2, axis=1))
                stats['gps_total_distance_m'] = float(np.sum(distances))

        return stats

    def format_compact_matrices(self, json_str: str) -> str:
        """Format matrices to be compact (one row per line)"""
        lines = json_str.split('\n')
        result = []
        i = 0

        while i < len(lines):
            line = lines[i]

            # Check if this is the start of a matrix
            if '"matrix": [' in line:
                result.append(line)
                i += 1

                # Process matrix rows
                while i < len(lines):
                    # Look for start of a row (line with just '[')
                    if lines[i].strip() == '[':
                        # Collect all values for this row
                        row_parts = []
                        j = i
                        while j < len(lines):
                            part = lines[j].strip()
                            row_parts.append(part)
                            if part.endswith('],') or part.endswith(']'):
                                break
                            j += 1

                        # Combine into single line
                        if row_parts:
                            # Get proper indentation (count spaces before '[')
                            indent_count = len(lines[i]) - len(lines[i].lstrip())
                            indent = ' ' * indent_count

                            # Format row as single line
                            row_values = []
                            is_last_row = False
                            for part in row_parts:
                                if part == '[':
                                    continue
                                elif part.endswith('],'):
                                    # Handle last value in non-last row
                                    val = part[:-2]  # Remove trailing ],
                                    if val:
                                        row_values.append(val)
                                    break
                                elif part.endswith(']'):
                                    # Handle last value in last row
                                    val = part[:-1]  # Remove trailing ]
                                    if val:
                                        row_values.append(val)
                                    is_last_row = True
                                    break
                                else:
                                    # Regular value
                                    val = part.rstrip(',')
                                    if val:
                                        row_values.append(val)

                            # Create properly formatted row
                            if is_last_row:
                                row_str = indent + '[' + ', '.join(row_values) + ']'
                            else:
                                row_str = indent + '[' + ', '.join(row_values) + '],'
                            result.append(row_str)

                            i = j + 1
                        else:
                            result.append(lines[i])
                            i += 1

                    elif lines[i].strip() == ']':
                        # End of matrix
                        result.append(lines[i])
                        i += 1
                        break
                    else:
                        result.append(lines[i])
                        i += 1
            else:
                result.append(line)
                i += 1

        return '\n'.join(result)

    def save(self, data: Dict) -> None:
        """Save processed data to JSON file with compact matrix formatting"""
        # First convert to JSON with standard formatting
        json_str = json.dumps(data, indent=2)

        # Then apply compact matrix formatting
        formatted_json = self.format_compact_matrices(json_str)

        # Write to file
        with open(self.output_file, 'w') as f:
            f.write(formatted_json)

        print(f"✅ Saved processed data to: {self.output_file}")

    def run(self) -> None:
        """Run the complete post-processing pipeline"""
        try:
            processed_data = self.process()
            self.save(processed_data)

            # Print summary
            stats = processed_data['metadata']['post_processing']['statistics']
            print("\n📊 Processing Summary:")
            print(f"  - Frames processed: {stats['total_frames']}")
            print(f"  - Total radar detections: {stats['total_radar_detections']}")
            print(f"  - Avg objects per frame: {stats['avg_objects_per_frame']:.1f}")

            if 'radar_bounds' in stats:
                print(f"  - Radar detection range:")
                print(f"    X: [{stats['radar_bounds']['min']['x']:.2f}, {stats['radar_bounds']['max']['x']:.2f}] m")
                print(f"    Y: [{stats['radar_bounds']['min']['y']:.2f}, {stats['radar_bounds']['max']['y']:.2f}] m")
                print(f"    Z: [{stats['radar_bounds']['min']['z']:.2f}, {stats['radar_bounds']['max']['z']:.2f}] m")

            if 'gps_total_distance_m' in stats:
                print(f"  - GPS total distance traveled: {stats['gps_total_distance_m']:.2f} m")

        except Exception as e:
            print(f"❌ Error during processing: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python post_process_radar_data.py <session_folder>")
        print("Example: python post_process_radar_data.py data/session_20250918_001815/")
        sys.exit(1)

    session_path = Path(sys.argv[1])

    if not session_path.exists():
        print(f"❌ Error: Session folder not found: {session_path}")
        sys.exit(1)

    if not session_path.is_dir():
        print(f"❌ Error: Path is not a directory: {session_path}")
        sys.exit(1)

    try:
        processor = RadarDataPostProcessor(session_path)
        processor.run()
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()