#!/usr/bin/env python3
"""
Create ground truth labels for radar-drone detection matching.
Drone was out of FOV, so all frames should have drone_detection_idx: -1
"""

import json
import sys
import math
import os

def calculate_distance(gps_pos, detection):
    """Calculate 3D Euclidean distance between GPS position and detection."""
    return math.sqrt(
        (gps_pos["x"] - detection["x"])**2 +
        (gps_pos["y"] - detection["y"])**2 +
        (gps_pos["z"] - detection["z"])**2
    )

def main(session_dir=None):
    """Create ground truth labels for all frames."""

    # Handle session directory argument or default
    if session_dir is None:
        # Check if called from pytest (sys.argv[0] contains pytest or test_pipeline)
        if len(sys.argv) > 1 and not sys.argv[1].endswith('.py') and 'pytest' not in sys.argv[0] and 'test_pipeline' not in sys.argv[1]:
            # Command line usage with session directory
            session_dir = sys.argv[1]
            input_file = os.path.join(session_dir, "post_processed_data_000.json")
            integrated_output_file = os.path.join(session_dir, "post_processed_with_ground_truth_data_000.json")
            output_file = "ground_truth_labels.json"
        else:
            # Default for backward compatibility with tests
            input_file = "data/all_negative_test_cases/post_processed_data_000.json"
            integrated_output_file = None
            output_file = "ground_truth_labels.json"
    else:
        input_file = os.path.join(session_dir, "post_processed_data_000.json")
        integrated_output_file = os.path.join(session_dir, "post_processed_with_ground_truth_data_000.json")
        output_file = "ground_truth_labels.json"

    try:
        # Load the JSON data
        with open(input_file, 'r') as f:
            data = json.load(f)

        frames = data["frames"]
        ground_truth = {}

        # Detection threshold: 30cm radius
        DETECTION_THRESHOLD = 0.30  # 30cm in meters

        # Process each frame to create ground truth labels
        for frame in frames:
            frame_key = f"frame_{frame['frame_number']}"

            # Get GPS position (convert from cm to meters)
            if "enu_coordinates" in frame:
                gps_pos = {
                    "x": frame["enu_coordinates"]["x_cm"] / 100.0,
                    "y": frame["enu_coordinates"]["y_cm"] / 100.0,
                    "z": frame["enu_coordinates"]["z_cm"] / 100.0
                }

                # Calculate distances and add ground truth to each detection
                closest_distance = float('inf')
                closest_obj_idx = -1
                drone_detection_idx = -1

                if frame["objects"]:
                    for obj_idx, obj in enumerate(frame["objects"]):
                        distance = calculate_distance(gps_pos, obj)

                        # Add ground truth to each detection
                        is_drone = distance <= DETECTION_THRESHOLD
                        obj["ground_truth"] = {
                            "is_drone": is_drone,
                            "distance_to_gps": distance
                        }

                        # Track closest detection
                        if distance < closest_distance:
                            closest_distance = distance
                            closest_obj_idx = obj_idx
                            if is_drone:
                                drone_detection_idx = obj_idx

                # Create frame-level ground truth summary
                frame_ground_truth = {
                    "drone_detection_idx": drone_detection_idx,
                    "closest_clutter_distance": closest_distance
                }

                ground_truth[frame_key] = {
                    "drone_detection_idx": drone_detection_idx,
                    "distance": closest_distance
                }

                # Add frame-level ground truth summary for integrated file
                frame["ground_truth"] = frame_ground_truth

            else:
                # No GPS data for this frame (shouldn't happen)
                # Mark all detections as not drone
                if frame["objects"]:
                    for obj in frame["objects"]:
                        obj["ground_truth"] = {
                            "is_drone": False,
                            "distance_to_gps": float('inf')
                        }

                frame_ground_truth = {
                    "drone_detection_idx": -1,
                    "closest_clutter_distance": float('inf')
                }

                ground_truth[frame_key] = {
                    "drone_detection_idx": -1,
                    "distance": float('inf')
                }

                # Add frame-level ground truth summary for integrated file
                frame["ground_truth"] = frame_ground_truth

        # Save ground truth labels
        with open(output_file, 'w') as f:
            json.dump(ground_truth, f, indent=2)

        # Save integrated file if session directory provided
        if integrated_output_file:
            with open(integrated_output_file, 'w') as f:
                json.dump(data, f, indent=2)

        print(f"Ground truth labels saved to {output_file}")
        if integrated_output_file:
            print(f"Integrated data with ground truth saved to {integrated_output_file}")
        print(f"Total frames processed: {len(ground_truth)}")

        # Summary statistics
        negative_labels = sum(1 for label in ground_truth.values() if label["drone_detection_idx"] == -1)
        positive_labels = sum(1 for label in ground_truth.values() if label["drone_detection_idx"] != -1)
        distances = [label["distance"] for label in ground_truth.values() if label["distance"] != float('inf')]

        if positive_labels > 0:
            print(f"Positive labels (drone detected): {positive_labels}/{len(ground_truth)} ({positive_labels/len(ground_truth)*100:.1f}%)")
            print(f"Negative labels (drone out of FOV): {negative_labels}/{len(ground_truth)} ({negative_labels/len(ground_truth)*100:.1f}%)")
        else:
            print(f"Negative labels (drone out of FOV): {negative_labels}/{len(ground_truth)} (100%)")

        if distances:
            print(f"GPS-to-closest-clutter distances: min={min(distances):.3f}m, max={max(distances):.3f}m, mean={sum(distances)/len(distances):.3f}m")

        return ground_truth

    except FileNotFoundError:
        print(f"Error: Could not find input file: {input_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in input file: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"Error: Missing expected key in data: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()