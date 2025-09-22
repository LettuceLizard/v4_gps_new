#!/usr/bin/env python3
"""
3D Visualization of Radar Objects and GPS Position
Displays post-processed radar and GPS data in interactive 3D space.
Coordinate system: ENU (East-North-Up) with base station at origin (0,0,0)
"""

import json
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import argparse
from pathlib import Path
from typing import List, Dict, Any
import matplotlib.animation as animation
from matplotlib.widgets import Slider, Button
import time

class Radar3DVisualizer:
    """Interactive 3D visualizer for radar objects and GPS trajectory"""

    def __init__(self, json_file_path: str):
        """Initialize visualizer with post-processed data file"""
        self.json_file = Path(json_file_path)
        self.data = None
        self.frames = None
        self.current_frame = 0

        # Visualization settings
        self.fig = None
        self.ax = None
        self.radar_scatter = None
        self.gps_scatter = None
        self.gps_trajectory = None
        self.base_to_rover_vector = None

        # Load and validate data
        self.load_data()

    def load_data(self):
        """Load and parse the JSON data file"""
        print(f"Loading data from {self.json_file}...")

        try:
            with open(self.json_file, 'r') as f:
                self.data = json.load(f)

            self.frames = self.data['frames']
            print(f"✅ Loaded {len(self.frames)} frames")

            # Print metadata info
            metadata = self.data.get('metadata', {})
            post_proc = metadata.get('post_processing', {})

            print(f"📊 Coordinate system: {post_proc.get('coordinate_system', 'Unknown')}")
            print(f"📏 Units: {post_proc.get('processed_units', 'Unknown')}")
            print(f"🎯 Origin: {post_proc.get('origin', 'Unknown')}")

            if 'statistics' in post_proc:
                stats = post_proc['statistics']
                print(f"📈 Total radar detections: {stats.get('total_radar_detections', 0)}")
                print(f"📈 Avg objects per frame: {stats.get('avg_objects_per_frame', 0):.2f}")

        except Exception as e:
            raise RuntimeError(f"Failed to load data: {e}")

    def extract_frame_data(self, frame_idx: int) -> Dict[str, Any]:
        """Extract radar objects and GPS position for a specific frame"""
        if frame_idx >= len(self.frames):
            return None

        frame = self.frames[frame_idx]

        # Extract radar objects
        radar_objects = []
        if 'objects' in frame and frame['objects']:
            for obj in frame['objects']:
                radar_objects.append({
                    'x': obj['x'],
                    'y': obj['y'],
                    'z': obj['z'],
                    'velocity': obj.get('velocity', 0)
                })

        # Extract GPS position
        gps_position = None
        if 'processed' in frame and 'gps_position_meters' in frame['processed']:
            gps_pos = frame['processed']['gps_position_meters']
            gps_position = {
                'x': gps_pos['x'],
                'y': gps_pos['y'],
                'z': gps_pos['z']
            }

        return {
            'frame_number': frame.get('frame_number', frame_idx),
            'timestamp': frame.get('radar_timestamp', 0),
            'radar_objects': radar_objects,
            'gps_position': gps_position,
            'num_objects': len(radar_objects)
        }

    def extract_gps_trajectory(self) -> List[Dict[str, float]]:
        """Extract complete GPS trajectory from all frames"""
        trajectory = []

        for frame in self.frames:
            if 'processed' in frame and 'gps_position_meters' in frame['processed']:
                gps_pos = frame['processed']['gps_position_meters']
                trajectory.append({
                    'x': gps_pos['x'],
                    'y': gps_pos['y'],
                    'z': gps_pos['z'],
                    'frame': frame.get('frame_number', 0),
                    'timestamp': frame.get('radar_timestamp', 0)
                })

        return trajectory

    def setup_3d_plot(self):
        """Setup the 3D matplotlib figure and axes"""
        self.fig = plt.figure(figsize=(14, 10))
        self.ax = self.fig.add_subplot(111, projection='3d')

        # Calculate plot bounds from metadata
        metadata = self.data.get('metadata', {})
        stats = metadata.get('post_processing', {}).get('statistics', {})

        if 'radar_bounds' in stats and 'gps_bounds' in stats:
            radar_bounds = stats['radar_bounds']
            gps_bounds = stats['gps_bounds']

            # Combine bounds with some padding
            x_min = min(radar_bounds['min']['x'], gps_bounds['min']['x']) - 0.5
            x_max = max(radar_bounds['max']['x'], gps_bounds['max']['x']) + 0.5
            y_min = min(radar_bounds['min']['y'], gps_bounds['min']['y']) - 0.5
            y_max = max(radar_bounds['max']['y'], gps_bounds['max']['y']) + 0.5
            z_min = min(radar_bounds['min']['z'], gps_bounds['min']['z']) - 0.5
            z_max = max(radar_bounds['max']['z'], gps_bounds['max']['z']) + 0.5
        else:
            # Default bounds
            x_min, x_max = -2, 2
            y_min, y_max = -2, 2
            z_min, z_max = -2, 2

        # Set axis limits and labels
        self.ax.set_xlim(x_min, x_max)
        self.ax.set_ylim(y_min, y_max)
        self.ax.set_zlim(z_min, z_max)

        self.ax.set_xlabel('East (m)', fontsize=12)
        self.ax.set_ylabel('North (m)', fontsize=12)
        self.ax.set_zlabel('Up (m)', fontsize=12)

        # Mark origin (base station)
        self.ax.scatter([0], [0], [0], c='black', s=100, marker='s',
                       label='Base Station (0,0,0)', alpha=0.8)

        # Setup legend with clear positioning
        self.ax.legend(loc='upper left', bbox_to_anchor=(0.02, 0.98), fontsize=10)

        # Setup title
        self.ax.set_title('3D Radar Objects and GPS Trajectory Visualization', fontsize=14, pad=20)

        return self.fig, self.ax

    def plot_frame(self, frame_idx: int):
        """Plot a specific frame with radar objects and GPS position"""
        frame_data = self.extract_frame_data(frame_idx)

        if frame_data is None:
            return

        # Clear previous radar objects and rover position (but keep trajectory)
        if self.radar_scatter is not None:
            self.radar_scatter.remove()
        if self.gps_scatter is not None:
            self.gps_scatter.remove()

        # Clear previous base-to-rover vector
        if self.base_to_rover_vector is not None:
            self.base_to_rover_vector.remove()
            self.base_to_rover_vector = None

        # Plot radar objects (detections from rover's perspective)
        radar_objects = frame_data['radar_objects']
        if radar_objects:
            radar_x = [obj['x'] for obj in radar_objects]
            radar_y = [obj['y'] for obj in radar_objects]
            radar_z = [obj['z'] for obj in radar_objects]

            # Color by velocity - create meaningful color mapping
            velocities = [obj['velocity'] for obj in radar_objects]

            # Use fixed color scheme for better interpretation
            self.radar_scatter = self.ax.scatter(radar_x, radar_y, radar_z,
                                               c=velocities, cmap='RdYlBu_r',  # Red=moving towards, Blue=moving away
                                               s=60, alpha=0.8,
                                               label=f'Radar Objects ({len(radar_objects)})')

            # Add colorbar for velocity interpretation
            if not hasattr(self, 'colorbar_added'):
                cbar = self.fig.colorbar(self.radar_scatter, ax=self.ax, shrink=0.6, aspect=20)
                cbar.set_label('Velocity (m/s)\n← Away | Towards →', fontsize=10)
                self.colorbar_added = True

        # Plot rover position and base-to-rover vector
        gps_pos = frame_data['gps_position']
        vector_length = 0.0
        if gps_pos:
            rover_x, rover_y, rover_z = gps_pos['x'], gps_pos['y'], gps_pos['z']

            # Plot rover position
            self.gps_scatter = self.ax.scatter([rover_x], [rover_y], [rover_z],
                                             c='red', s=150, marker='^',
                                             label=f'Rover Position',
                                             alpha=0.9, edgecolors='darkred', linewidth=2)

            # Draw base-to-rover vector (arrow from base station to rover)
            self.base_to_rover_vector = self.ax.quiver(0, 0, 0,  # Start at base (0,0,0)
                                                      rover_x, rover_y, rover_z,  # Vector to rover
                                                      color='blue', alpha=0.7,
                                                      arrow_length_ratio=0.1,
                                                      linewidth=3,
                                                      label='Base→Rover Vector')

            # Calculate and display vector length
            vector_length = np.sqrt(rover_x**2 + rover_y**2 + rover_z**2)
            self.ax.text(rover_x/2, rover_y/2, rover_z/2 + 0.1,
                        f'{vector_length:.3f}m',
                        fontsize=10, color='blue', weight='bold')

        # Update title with frame info
        title = f'Frame {frame_idx}: {frame_data["num_objects"]} Objects'
        if vector_length > 0:
            title += f' | Rover Distance: {vector_length:.3f}m'
        title += f' | Time: {frame_data["timestamp"]:.3f}s'

        self.ax.set_title(title, fontsize=14, pad=20)

        self.fig.canvas.draw()

    def plot_gps_trajectory(self):
        """Plot the complete GPS trajectory"""
        trajectory = self.extract_gps_trajectory()

        if not trajectory:
            print("⚠️ No GPS trajectory data found")
            return

        # Extract coordinates
        traj_x = [point['x'] for point in trajectory]
        traj_y = [point['y'] for point in trajectory]
        traj_z = [point['z'] for point in trajectory]

        # Plot trajectory line
        self.gps_trajectory = self.ax.plot(traj_x, traj_y, traj_z,
                                          'r-', alpha=0.6, linewidth=2,
                                          label='GPS Trajectory')[0]

        print(f"✅ Plotted GPS trajectory with {len(trajectory)} points")

    def create_interactive_plot(self):
        """Create interactive plot with frame navigation controls"""
        self.setup_3d_plot()
        self.plot_gps_trajectory()

        # Create slider for frame navigation
        ax_slider = plt.axes([0.1, 0.02, 0.6, 0.03])
        slider = Slider(ax_slider, 'Frame', 0, len(self.frames)-1,
                       valinit=0, valfmt='%d')

        # Create control buttons
        ax_prev = plt.axes([0.72, 0.02, 0.08, 0.04])
        ax_next = plt.axes([0.81, 0.02, 0.08, 0.04])
        ax_play = plt.axes([0.90, 0.02, 0.08, 0.04])

        btn_prev = Button(ax_prev, 'Prev')
        btn_next = Button(ax_next, 'Next')
        btn_play = Button(ax_play, 'Play')

        self.current_frame = 0
        self.plot_frame(self.current_frame)

        # Event handlers
        def update_frame(val):
            frame_idx = int(slider.val)
            self.current_frame = frame_idx
            self.plot_frame(frame_idx)

        def prev_frame(event):
            if self.current_frame > 0:
                self.current_frame -= 1
                slider.set_val(self.current_frame)

        def next_frame(event):
            if self.current_frame < len(self.frames) - 1:
                self.current_frame += 1
                slider.set_val(self.current_frame)

        def play_animation(event):
            for i in range(self.current_frame, len(self.frames)):
                self.current_frame = i
                slider.set_val(i)
                plt.pause(0.1)  # 10 FPS playback

        # Connect events
        slider.on_changed(update_frame)
        btn_prev.on_clicked(prev_frame)
        btn_next.on_clicked(next_frame)
        btn_play.on_clicked(play_animation)

        plt.show()

    def generate_summary_stats(self):
        """Generate and print summary statistics"""
        print("\n" + "="*60)
        print("📊 VISUALIZATION SUMMARY")
        print("="*60)

        metadata = self.data.get('metadata', {})
        stats = metadata.get('post_processing', {}).get('statistics', {})

        print(f"📁 Data file: {self.json_file.name}")
        print(f"🎬 Total frames: {len(self.frames)}")
        print(f"🎯 Total radar detections: {stats.get('total_radar_detections', 0)}")
        print(f"📈 Average objects per frame: {stats.get('avg_objects_per_frame', 0):.2f}")
        print(f"📏 GPS total distance: {stats.get('gps_total_distance_m', 0):.3f} m")

        # Calculate frame rate
        if len(self.frames) > 1:
            start_time = self.frames[0].get('radar_timestamp', 0)
            end_time = self.frames[-1].get('radar_timestamp', 0)
            duration = end_time - start_time
            frame_rate = len(self.frames) / duration if duration > 0 else 0
            print(f"⏱️  Estimated frame rate: {frame_rate:.1f} Hz")
            print(f"⏱️  Session duration: {duration:.1f} seconds")

        if 'radar_bounds' in stats:
            bounds = stats['radar_bounds']
            print(f"📍 Radar detection bounds:")
            print(f"   X: {bounds['min']['x']:.3f} to {bounds['max']['x']:.3f} m")
            print(f"   Y: {bounds['min']['y']:.3f} to {bounds['max']['y']:.3f} m")
            print(f"   Z: {bounds['min']['z']:.3f} to {bounds['max']['z']:.3f} m")

        print("="*60)

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description='3D Visualization of Radar and GPS Data')
    parser.add_argument('session_path', help='Path to session directory (e.g. data/session_20250918_014126) or JSON file')
    parser.add_argument('--frame', type=int, default=None,
                       help='Display specific frame (default: interactive mode)')
    parser.add_argument('--stats-only', action='store_true',
                       help='Only print statistics without visualization')

    args = parser.parse_args()

    # Handle both session directory and direct JSON file paths
    input_path = Path(args.session_path)

    if input_path.is_dir():
        # Session directory provided - find the post-processed JSON file
        json_file = input_path / "post_processed_data_000.json"
        if not json_file.exists():
            print(f"❌ Error: No post-processed data found in session directory: {input_path}")
            print(f"   Expected file: {json_file}")
            return 1
        print(f"📁 Using session directory: {input_path}")
    elif input_path.suffix == '.json':
        # JSON file provided directly
        json_file = input_path
        if not json_file.exists():
            print(f"❌ Error: JSON file not found: {json_file}")
            return 1
    else:
        print(f"❌ Error: Expected session directory or .json file, got: {input_path}")
        return 1

    try:
        # Initialize visualizer
        visualizer = Radar3DVisualizer(str(json_file))

        # Print summary statistics
        visualizer.generate_summary_stats()

        if args.stats_only:
            return 0

        if args.frame is not None:
            # Display specific frame
            visualizer.setup_3d_plot()
            visualizer.plot_gps_trajectory()
            visualizer.plot_frame(args.frame)
            plt.show()
        else:
            # Interactive mode
            print(f"\n🎮 Starting interactive 3D visualization...")
            print(f"   Use slider to navigate frames")
            print(f"   Use Prev/Next buttons for frame control")
            print(f"   Use Play button for animation")
            visualizer.create_interactive_plot()

        return 0

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())