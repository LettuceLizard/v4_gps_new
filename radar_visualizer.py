#!/usr/bin/env python3
"""
Minimal AWR2944 Radar Range-Doppler Visualizer
Core 80% functionality in 20% of the code
"""

import json
import tkinter as tk
from tkinter import filedialog, messagebox
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import numpy as np
from pathlib import Path
import signal
import sys

class RadarVisualizer:
    def __init__(self, session_path=None):
        self.root = tk.Tk()
        self.root.title("AWR2944 Radar Range-Doppler Visualizer")
        self.root.geometry("1080x1920")

        self.data = None
        self.current_frame = 0
        self.total_frames = 0
        self.autoplay = False
        self.autoplay_speed = 100  # milliseconds
        self.autoplay_job = None
        self.session_path = session_path

        self.setup_gui()

        # Auto-load if session path provided
        if self.session_path:
            self.load_data_from_path(self.session_path)

    def setup_gui(self):
        # Top controls
        controls = tk.Frame(self.root)
        controls.pack(pady=10)

        tk.Button(controls, text="Select Session Folder", command=self.load_data).pack(side=tk.LEFT, padx=5)

        self.frame_label = tk.Label(controls, text="Frame: 0/0")
        self.frame_label.pack(side=tk.LEFT, padx=10)

        tk.Button(controls, text="←", command=self.prev_frame).pack(side=tk.LEFT, padx=2)
        tk.Button(controls, text="→", command=self.next_frame).pack(side=tk.LEFT, padx=2)

        self.autoplay_btn = tk.Button(controls, text="▶ Play", command=self.toggle_autoplay)
        self.autoplay_btn.pack(side=tk.LEFT, padx=5)

        tk.Button(controls, text="Exit", command=self.cleanup_and_exit).pack(side=tk.LEFT, padx=5)

        # Main plot area
        self.fig, self.axes = plt.subplots(4, 4, figsize=(12, 16))
        self.fig.suptitle('Range-Doppler Slices (31×16 each)', fontsize=14)

        self.canvas = FigureCanvasTkAgg(self.fig, self.root)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # Flatten axes for easy indexing
        self.axes = self.axes.flatten()

    def load_data(self):
        folder = filedialog.askdirectory(title="Select Session Folder")
        if not folder:
            return
        self.load_data_from_path(folder)

    def load_data_from_path(self, folder):
        json_file = Path(folder) / "data_000.json"
        if not json_file.exists():
            messagebox.showerror("Error", f"data_000.json not found in {folder}")
            return

        try:
            with open(json_file, 'r') as f:
                self.data = json.load(f)

            self.total_frames = len(self.data['frames'])
            self.current_frame = 0
            self.update_display()
            messagebox.showinfo("Success", f"Loaded {self.total_frames} frames from {folder}")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load data: {e}")

    def prev_frame(self):
        if self.data and self.current_frame > 0:
            self.current_frame -= 1
            self.update_display()

    def next_frame(self):
        if self.data and self.current_frame < self.total_frames - 1:
            self.current_frame += 1
            self.update_display()

    def update_display(self):
        if not self.data:
            return

        frame_data = self.data['frames'][self.current_frame]
        objects = frame_data['objects']

        self.frame_label.config(text=f"Frame: {self.current_frame + 1}/{self.total_frames}")

        # Clear all subplots
        for ax in self.axes:
            ax.clear()
            ax.set_visible(False)

        # Show up to 16 objects
        for i, obj in enumerate(objects[:16]):
            if i >= 16:
                break

            ax = self.axes[i]
            ax.set_visible(True)

            slice_data = obj['range_doppler_slice']
            matrix = np.array(slice_data['matrix'])

            # Show heatmap with true 1:1 pixel mapping
            rows, cols = matrix.shape
            if i == 0:  # Debug info for first object only
                print(f"Matrix shape: {rows}x{cols}")

            # Set exact pixel boundaries and force equal aspect for true resolution
            im = ax.imshow(matrix, cmap='viridis', interpolation='nearest',
                          extent=[0, cols, rows, 0], aspect='equal')

            # Set axis to show exact bin indices
            ax.set_xlim(0, cols)
            ax.set_ylim(rows, 0)

            # Highlight detected position
            detected_range = slice_data['detected_range_idx']
            detected_doppler = slice_data['detected_doppler_idx']
            range_start = slice_data['range_window_start']

            # Calculate position in matrix
            matrix_row = detected_range - range_start
            matrix_col = detected_doppler

            if 0 <= matrix_row < 31 and 0 <= matrix_col < 16:
                rect = patches.Rectangle((matrix_col-0.5, matrix_row-0.5), 1, 1,
                                       linewidth=2, edgecolor='red', facecolor='none')
                ax.add_patch(rect)

            ax.set_title(f'Obj {i+1}: ({obj["x"]:.2f}, {obj["y"]:.2f}, {obj["z"]:.2f})')
            ax.set_xlabel('Doppler Bins')
            ax.set_ylabel('Range Bins')

        self.canvas.draw()

    def toggle_autoplay(self):
        self.autoplay = not self.autoplay
        if self.autoplay:
            self.autoplay_btn.config(text="⏸ Pause")
            self.autoplay_loop()
        else:
            self.autoplay_btn.config(text="▶ Play")
            if self.autoplay_job:
                self.root.after_cancel(self.autoplay_job)
                self.autoplay_job = None

    def autoplay_loop(self):
        try:
            if not self.autoplay or not self.data:
                return

            # Move to next frame
            if self.current_frame < self.total_frames - 1:
                self.current_frame += 1
            else:
                self.current_frame = 0  # Loop back to start

            self.update_display()

            # Schedule next frame using lambda to avoid callback corruption
            if self.autoplay:
                self.autoplay_job = self.root.after(self.autoplay_speed, lambda: self.autoplay_loop())
        except Exception as e:
            print(f"Autoplay error: {e}")
            self.autoplay = False
            self.autoplay_btn.config(text="▶ Play")

    def cleanup_and_exit(self):
        """Properly cleanup and exit"""
        self.autoplay = False
        if self.autoplay_job:
            self.root.after_cancel(self.autoplay_job)
        plt.close('all')
        self.root.quit()
        self.root.destroy()

    def run(self):
        # Handle Ctrl+C gracefully
        def signal_handler(sig, frame):
            self.cleanup_and_exit()
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        try:
            self.root.protocol("WM_DELETE_WINDOW", self.cleanup_and_exit)
            self.root.mainloop()
        except KeyboardInterrupt:
            self.cleanup_and_exit()

if __name__ == "__main__":
    # Check if a session path was provided as command line argument
    session_path = None
    if len(sys.argv) > 1:
        session_path = sys.argv[1]
        print(f"Loading session from: {session_path}")

    app = RadarVisualizer(session_path)
    app.run()