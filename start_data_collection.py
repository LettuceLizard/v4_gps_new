#!/usr/bin/env python3
"""
GPS Data Collection with TCP Server
Contains GPS data processing utilities for UM980 receivers and coordinate transformations.
Includes TCP server functionality for receiving sensor data.
"""

import serial
import threading
import time
import numpy as np
from pyproj import Transformer as ProjTransformer
from ahrs.common.frames import ecef2enu
from enum import Enum
import socket
import struct
import sys
import json
import argparse
from typing import Optional, Callable, Any, Dict
import os
from datetime import datetime
from pathlib import Path
from collections import deque
import queue

# --- ASCII Protocol Classes ---
class ASCIIHEADER:
    def __init__(self, message, cpu_idle, time_ref, time_status, week_number,
                 milliseconds, reserved, version, leap_sec, output_delay):
        self.message = message
        self.cpu_idle = cpu_idle
        self.time_ref = time_ref
        self.time_status = time_status
        self.week_number = week_number
        self.milliseconds = milliseconds
        self.reserved = reserved
        self.version = version
        self.leap_sec = leap_sec
        self.output_delay = output_delay

    def __str__(self):
        return (f"HEADER:\n"
                f"  message: {self.message}\n"
                f"  cpu_idle: {self.cpu_idle}\n"
                f"  time_ref: {self.time_ref}\n"
                f"  time_status: {self.time_status}\n"
                f"  week_number: {self.week_number}\n"
                f"  milliseconds: {self.milliseconds}\n"
                f"  reserved: {self.reserved}\n"
                f"  version: {self.version}\n"
                f"  leap_sec: {self.leap_sec}\n"
                f"  output_delay: {self.output_delay}")

class BESTNAVXYZA:
    def __init__(self, header: ASCIIHEADER, sol_status, pos_type, pos_x, pos_y, pos_z,
                 pos_x_stdev, pos_y_stdev, pos_z_stdev, vel_status, vel_type, vel_x, vel_y,
vel_z,
                 vel_x_stdev, vel_y_stdev, vel_z_stdev, stn_id, diff_age, sol_age, sol_age2,
                 num_svs, num_soln_svs, num_gg_l1, num_soln_multi_svs, reserved, ext_sol_stat,
                 galileo_bds3_sig_mask, gps_glonass_bds2_sig_mask, crc):
        self.header = header
        self.sol_status = sol_status
        self.pos_type = pos_type
        self.pos_x = float(pos_x) if pos_x else 0.0
        self.pos_y = float(pos_y) if pos_y else 0.0
        self.pos_z = float(pos_z) if pos_z else 0.0
        self.pos_x_stdev = float(pos_x_stdev) if pos_x_stdev else 0.0
        self.pos_y_stdev = float(pos_y_stdev) if pos_y_stdev else 0.0
        self.pos_z_stdev = float(pos_z_stdev) if pos_z_stdev else 0.0
        self.vel_status = vel_status
        self.vel_type = vel_type
        self.vel_x = float(vel_x) if vel_x else 0.0
        self.vel_y = float(vel_y) if vel_y else 0.0
        self.vel_z = float(vel_z) if vel_z else 0.0
        self.vel_x_stdev = float(vel_x_stdev) if vel_x_stdev else 0.0
        self.vel_y_stdev = float(vel_y_stdev) if vel_y_stdev else 0.0
        self.vel_z_stdev = float(vel_z_stdev) if vel_z_stdev else 0.0
        self.stn_id = stn_id
        self.diff_age = float(diff_age) if diff_age else 0.0
        self.sol_age = float(sol_age) if sol_age else 0.0
        self.sol_age2 = float(sol_age2) if sol_age2 else 0.0
        self.num_svs = int(float(num_svs)) if num_svs else 0
        self.num_soln_svs = int(float(num_soln_svs)) if num_soln_svs else 0
        self.num_gg_l1 = int(float(num_gg_l1)) if num_gg_l1 else 0
        self.num_soln_multi_svs = int(float(num_soln_multi_svs)) if num_soln_multi_svs else 0
        self.reserved = reserved
        self.ext_sol_stat = ext_sol_stat
        self.galileo_bds3_sig_mask = galileo_bds3_sig_mask
        self.gps_glonass_bds2_sig_mask = gps_glonass_bds2_sig_mask
        self.crc = crc

class BESTNAVA:
    def __init__(self, header: ASCIIHEADER, sol_status, pos_type, lat, lon, hgt, undulation, datum_id,
                 lat_stdev, lon_stdev, hgt_stdev, stn_id, diff_age, sol_age, num_svs, num_soln_svs,
                 reserved1, reserved2, reserved3, ext_sol_stat, galileo_bds3_sig_mask, gps_glonass_bds2_sig_mask,
                 vsol_status, vel_type, latency, age, hor_spd, trk_gnd, vert_spd, vert_spd_stdev, hor_spd_stdev,
                 crc):

        self.header = header
        self.sol_status = sol_status
        self.pos_type = pos_type
        self.lat = lat
        self.lon = lon
        self.hgt = hgt
        self.undulation = undulation
        self.datum_id = datum_id
        self.lat_stdev = lat_stdev
        self.lon_stdev = lon_stdev
        self.hgt_stdev = hgt_stdev
        self.stn_id = stn_id
        self.diff_age = diff_age
        self.sol_age = sol_age
        self.num_svs = num_svs
        self.num_soln_svs = num_soln_svs
        self.reserved1 = reserved1
        self.reserved2 = reserved2
        self.reserved3 = reserved3
        self.ext_sol_stat = ext_sol_stat
        self.galileo_bds3_sig_mask = galileo_bds3_sig_mask
        self.gps_glonass_bds2_sig_mask = gps_glonass_bds2_sig_mask
        self.vsol_status = vsol_status
        self.vel_type = vel_type
        self.latency = latency
        self.age = age
        self.hor_spd = hor_spd
        self.trk_gnd = trk_gnd
        self.vert_spd = vert_spd
        self.vert_spd_stdev = vert_spd_stdev
        self.hor_spd_stdev = hor_spd_stdev
        self.crc = crc

class NMEACommandType(Enum):
    BESTNAVXYZA = "BESTNAVXYZA"
    BESTNAVA = "BESTNAVA"

class NMEACommandParser():

    @classmethod
    def _parse_header(cls, header_part):
        """Parse the common header format"""
        header_fields = header_part.split(',')
        if len(header_fields) >= 10:
            return ASCIIHEADER(
                message=header_fields[0].replace('#', ''),
                cpu_idle=header_fields[1],
                time_ref=header_fields[2],
                time_status=header_fields[3],
                week_number=header_fields[4],
                milliseconds=header_fields[5],
                reserved=header_fields[6],
                version=header_fields[7],
                leap_sec=header_fields[8],
                output_delay=header_fields[9]
            )

    @classmethod
    def parse_bestnava(cls, line: str) -> BESTNAVA:
        try:
            parts = line.split(";")
            if len(parts) != 2:
                return None

            header = cls._parse_header(parts[0])
            if not header:
                return None

            data_fields = parts[1].split(",")

            if len(data_fields) > 0 and '*' in data_fields[-1]:
                crc_part = data_fields[-1].split('*')
                data_fields[-1] = crc_part[0]
                data_fields.append(crc_part[1] if len(crc_part) > 1 else '')
            else:
                data_fields.append("")

            # BESTNAVA constructor needs exactly 31 parameters after header
            # Validate we have enough fields before attempting to construct
            if len(data_fields) < 31:
                # Pad data_fields to ensure we have enough fields
                while len(data_fields) < 31:
                    data_fields.append('')

            # Truncate if we have too many fields (corrupted message)
            if len(data_fields) > 31:
                data_fields = data_fields[:31]

            nav_msg = BESTNAVA(
                header,
                *data_fields
            )

            return nav_msg
        except Exception:
            # Silently return None for corrupted messages
            # This prevents the thread from crashing
            return None

    @classmethod
    def parse_bestnavxyza(cls, line: str) -> BESTNAVXYZA:
        """Parse BESTNAVXYZA message"""
        parts = line.split(';')
        if len(parts) != 2:
            return None

        # Parse header
        header = cls._parse_header(parts[0])

        data_fields = parts[1].split(',')

        # Remove CRC from last field if present
        if len(data_fields) > 0 and '*' in data_fields[-1]:
            crc_part = data_fields[-1].split('*')
            data_fields[-1] = crc_part[0]
            data_fields.append(crc_part[1] if len(crc_part) > 1 else '')
        else:
            data_fields.append('')

        # Pad data_fields to ensure we have enough fields
        while len(data_fields) < 29:
            data_fields.append('')

        # Create BESTNAVXYZA object
        nav_msg = BESTNAVXYZA(
            header,
            data_fields[0],   # sol_status
            data_fields[1],   # pos_type
            data_fields[2],   # pos_x
            data_fields[3],   # pos_y
            data_fields[4],   # pos_z
            data_fields[5],   # pos_x_stdev
            data_fields[6],   # pos_y_stdev
            data_fields[7],   # pos_z_stdev
            data_fields[8],   # vel_status
            data_fields[9],   # vel_type
            data_fields[10],  # vel_x
            data_fields[11],  # vel_y
            data_fields[12],  # vel_z
            data_fields[13],  # vel_x_stdev
            data_fields[14],  # vel_y_stdev
            data_fields[15],  # vel_z_stdev
            data_fields[16].replace('"', ''),  # stn_id (remove quotes)
            data_fields[17],  # diff_age
            data_fields[18],  # sol_age
            data_fields[19],  # sol_age2
            data_fields[20],  # num_svs
            data_fields[21],  # num_soln_svs
            data_fields[22],  # num_gg_l1
            data_fields[23],  # num_soln_multi_svs
            data_fields[24],  # reserved
            data_fields[25],  # ext_sol_stat
            data_fields[26] if len(data_fields) > 26 else '',  # galileo_bds3_sig_mask
            data_fields[27] if len(data_fields) > 27 else '',  # gps_glonass_bds2_sig_mask
            data_fields[28]   # crc
        )

        return nav_msg

class UM980Reader:

    class GPSData:
        def __init__(self):
            self.x_ecef = None
            self.y_ecef = None
            self.z_ecef = None
            self.lat = None
            self.lon = None
            self.height = None
            self.pos_type = None
            self.undulation = None
        def __str__(self):
            return f"GPSData(x_ecef={self.x_ecef}, y_ecef={self.y_ecef}, z_ecef={self.z_ecef}, lat={self.lat}, lon={self.lon}, height={self.height}, undulation={self.undulation}, pos_type={self.pos_type})"

    def __init__(self, port: str, baudrate: int, debug_label=""):
        self.port = port
        self.baudrate = baudrate
        self.serial_connection = None
        self.debug_label = debug_label

        self.running = False
        self.read_thread = None
        self.rover_data = self.GPSData()

    def start(self):
        try:
            # Use shorter timeout for faster recovery from incomplete messages
            self.serial_connection = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=0.5  # Reduced timeout for high-frequency data
            )
        except Exception as e:
            print(f"Could not connect to serial port {self.port}: {e}")
            raise e

        try:
            self.read_thread = threading.Thread(target=self._read_loop, daemon=True)
            self.running = True
            self.read_thread.start()
        except Exception as e:
            print(f"Could not start thread: {e}")
            raise e

    def stop(self):
        if self.running:
            self.running = False
            self.read_thread.join()

    def _read_loop(self):
        data_count = 0
        error_count = 0
        last_error_time = 0

        while self.running:
            try:
                # Check for serial buffer overflow (high-frequency data issue)
                if self.serial_connection.in_waiting > 8192:
                    # Clear buffer if it's getting too full
                    self.serial_connection.reset_input_buffer()
                    error_count += 1
                    current_time = time.time()
                    if current_time - last_error_time > 5:
                        print(f"[{self.debug_label}] Warning: Serial buffer overflow cleared (count: {error_count})")
                        last_error_time = current_time
                    continue

                # Read raw bytes first
                raw_data = self.serial_connection.readline()
                if not raw_data:
                    continue

                data_count += 1

                # Try to decode with error handling for invalid UTF-8
                try:
                    data = raw_data.decode('utf-8', errors='replace').strip()
                except UnicodeDecodeError:
                    # Skip non-UTF-8 data (likely binary protocol data)
                    continue

                # Skip empty lines or lines that don't contain expected commands
                if not data or (NMEACommandType.BESTNAVA.value not in data and NMEACommandType.BESTNAVXYZA.value not in data):
                    continue

                # Parse with error protection
                try:
                    if NMEACommandType.BESTNAVA.value in data:
                        bestnava = NMEACommandParser.parse_bestnava(data)
                        if bestnava and bestnava.lat and bestnava.lon:
                            self.rover_data.lat = float(bestnava.lat)
                            self.rover_data.lon = float(bestnava.lon)
                            self.rover_data.height = float(bestnava.hgt)
                            self.rover_data.undulation = float(bestnava.undulation)
                            self.rover_data.pos_type = bestnava.pos_type
                    elif NMEACommandType.BESTNAVXYZA.value in data:
                        bestnavxyz = NMEACommandParser.parse_bestnavxyza(data)
                        if bestnavxyz:
                            self.rover_data.x_ecef = bestnavxyz.pos_x
                            self.rover_data.y_ecef = bestnavxyz.pos_y
                            self.rover_data.z_ecef = bestnavxyz.pos_z
                            self.rover_data.pos_type = bestnavxyz.pos_type
                except (ValueError, TypeError, AttributeError):
                    # Ignore parsing errors for individual messages
                    # This keeps the thread running even with corrupted data
                    error_count += 1
                    continue

            except serial.SerialException as e:
                # Serial port disconnected - this is fatal
                print(f"[{self.debug_label}] Serial port error: {e}")
                if self.serial_connection and self.serial_connection.is_open:
                    self.serial_connection.close()
                self.running = False
                break
            except Exception as e:
                # Other errors - log but keep trying
                error_count += 1
                current_time = time.time()
                if current_time - last_error_time > 5:
                    print(f"[{self.debug_label}] Read loop error: {e} (errors: {error_count})")
                    last_error_time = current_time
                # Keep the thread running
                continue

        # Clean up on exit
        if self.serial_connection and self.serial_connection.is_open:
            self.serial_connection.close()

    def get_data(self) -> "UM980Reader.GPSData":
        return self.rover_data

def get_base_station_coords(port, baudrate=115200):
    """
    Gets base station coordinates using ASCII protocol (BESTNAVA messages).
    Waits for a position with acceptable accuracy.
    """
    print(f"--- Reading Base Station coordinates from {port}... ---")
    base_reader = None
    stop_capture = False

    try:
        base_reader = UM980Reader(port, baudrate, "BASE")
        base_reader.start()
        print("✅ Base station reader started")

        # Wait for valid position data
        attempts = 0
        max_attempts = 30  # 30 seconds timeout

        while not stop_capture and attempts < max_attempts:
            try:
                base_data = base_reader.get_data()

                if base_data.lat is not None and base_data.pos_type is not None:
                    pos_type = base_data.pos_type
                    print(f"\r📍 Base station status: {pos_type}", end="")

                    # Accept any position type that has coordinates
                    if pos_type in ['NARROW_INT', 'WIDE_INT', 'L1_FLOAT', 'IONOFREE_FLOAT', 'SINGLE', 'FIXEDPOS']:
                        print(f"\n✅ Base station coordinates acquired: ({base_data.lat:.6f}, {base_data.lon:.6f}, {base_data.height:.2f})")
                        print(f"✅ Position type: {pos_type}")
                        base_reader.stop()
                        return base_data  # Return GPSData object directly

                time.sleep(0.5)  # Shorter sleep for better responsiveness
                attempts += 1

            except KeyboardInterrupt:
                print("\n🛑 Base station coordinate reading interrupted")
                if base_reader:
                    base_reader.stop()
                return None

        print(f"\n⚠️ Timeout waiting for base station coordinates after {max_attempts//2}s")
        if base_reader:
            base_reader.stop()
        return None

    except KeyboardInterrupt:
        print("\n🛑 Base station coordinate reading interrupted")
        if base_reader:
            base_reader.stop()
        return None
    except Exception as e:
        print(f"\n❌ ERROR: Could not read base station coordinates: {e}")
        if base_reader:
            base_reader.stop()
        return None

def create_enu_transformer(base_coords):
    return ProjTransformer.from_crs("EPSG:4979", "EPSG:4978", always_xy=True)

def convert_gps_to_enu_cm(base_coords, rover_data, transformer=None):
    """
    Converts the rover's GPS coordinates to a local East, North, Up (ENU)
    frame relative to the base station using ECEF coordinates and ahrs.ecef2enu.
    Accepts both GPSData objects and dict format for backward compatibility.
    """
    try:
        # Convert base and rover LLA to ECEF using pyproj
        ecef_transformer = ProjTransformer.from_crs("EPSG:4979", "EPSG:4978", always_xy=True)

        # Handle both GPSData objects and dict format
        if hasattr(base_coords, 'lat'):  # GPSData object
            base_lon, base_lat, base_alt = base_coords.lon, base_coords.lat, base_coords.height
        else:  # Dict format (backward compatibility)
            # Try both possible key formats for flexibility
            base_lon = base_coords.get('lon', base_coords.get('longitude'))
            base_lat = base_coords.get('lat', base_coords.get('latitude'))
            base_alt = base_coords.get('alt', base_coords.get('altitude'))

        if hasattr(rover_data, 'lat'):  # GPSData object
            rover_lon, rover_lat, rover_alt = rover_data.lon, rover_data.lat, rover_data.height
        else:  # Dict format (backward compatibility)
            rover_lon, rover_lat, rover_alt = rover_data['longitude'], rover_data['latitude'], rover_data['altitude']

        base_x, base_y, base_z = ecef_transformer.transform(base_lon, base_lat, base_alt)
        rover_x, rover_y, rover_z = ecef_transformer.transform(rover_lon, rover_lat, rover_alt)

        # Calculate ECEF difference
        diff_ecef = np.array([
            rover_x - base_x,
            rover_y - base_y,
            rover_z - base_z
        ])

        # Use ahrs.ecef2enu to get ENU transformation matrix
        enu_matrix = ecef2enu(lat=base_lat, lon=base_lon)

        # Apply transformation
        enu_coords = enu_matrix.dot(diff_ecef)

        # Flip only the North/South axis to align GPS with radar coordinate system
        # GPS South becomes North in the radar reference frame
        enu_coords[1] = -enu_coords[1]  # Flip North/South only

        return {"x_cm": enu_coords[0] * 100, "y_cm": enu_coords[1] * 100, "z_cm": enu_coords[2] * 100}

    except Exception as e:
        print(f"!!! ERROR during ENU conversion: {e}")
        return {"x_cm": 0.0, "y_cm": 0.0, "z_cm": 0.0}

# --- TCP Server Components (from tcp_server.py) ---
class DataMode(Enum):
    RAW = "raw"              # Raw bytes, no parsing
    STRUCTURED = "structured" # Expects header with data size/count
    JSON = "json"            # JSON messages
    CUSTOM = "custom"        # Custom parsing function

class GeneralTCPServer:
    def __init__(self, host: str = '0.0.0.0', port: int = 7,
                 data_mode: DataMode = DataMode.RAW,
                 buffer_size: int = 4096,
                 timeout: float = 1.0,
                 header_format: str = '<I',
                 data_format: str = '<f',
                 custom_parser: Optional[Callable] = None,
                 verbose: bool = True):
        """
        Initialize the general TCP server

        Args:
            host: Server host to bind to
            port: Server port to listen on
            data_mode: How to interpret incoming data
            buffer_size: Default buffer size for raw data
            timeout: Socket timeout in seconds
            header_format: Struct format for header (structured mode)
            data_format: Struct format for data elements (structured mode)
            custom_parser: Custom parsing function for CUSTOM mode
            verbose: Enable verbose logging
        """
        self.host = host
        self.port = port
        self.data_mode = data_mode
        self.buffer_size = buffer_size
        self.timeout = timeout
        self.header_format = header_format
        self.data_format = data_format
        self.custom_parser = custom_parser
        self.verbose = verbose

        self.server_socket = None
        self.running = False
        self.client_handlers = []

    def log(self, message: str):
        """Log message if verbose mode is enabled"""
        if self.verbose:
            timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
            print(f"[{timestamp}] {message}")

    def receive_data(self, client_socket: socket.socket, size: int) -> bytes:
        """Receive exactly 'size' bytes from the client socket"""
        data = b''
        while len(data) < size:
            chunk = client_socket.recv(size - len(data))
            if not chunk:
                raise ConnectionError("Connection lost while receiving data")
            data += chunk
        return data

    def parse_raw_data(self, client_socket: socket.socket) -> bytes:
        """Parse raw data - just receive buffer_size bytes"""
        return client_socket.recv(self.buffer_size)

    def parse_structured_data(self, client_socket: socket.socket) -> Dict[str, Any]:
        """Parse structured data with header indicating data size/count"""
        # Receive header
        header_size = struct.calcsize(self.header_format)
        header_data = self.receive_data(client_socket, header_size)
        header_values = struct.unpack(self.header_format, header_data)

        # Assume first header value is count/size
        data_count = header_values[0]

        if data_count == 0:
            return {"header": header_values, "data": [], "count": 0}

        # Calculate data size based on format and count
        data_element_size = struct.calcsize(self.data_format)
        total_data_size = data_element_size * data_count

        # Receive data
        data_bytes = self.receive_data(client_socket, total_data_size)

        # Unpack data
        format_str = '<' + self.data_format.lstrip('<>!@=') * data_count
        data_values = struct.unpack(format_str, data_bytes[:struct.calcsize(format_str)])

        return {
            "header": header_values,
            "data": data_values,
            "count": data_count,
            "raw_bytes": data_bytes
        }

    def parse_json_data(self, client_socket: socket.socket) -> Dict[str, Any]:
        """Parse JSON data - receive until complete JSON message"""
        data = b''
        while True:
            chunk = client_socket.recv(1024)
            if not chunk:
                break
            data += chunk

            # Try to parse as JSON
            try:
                message = data.decode('utf-8')
                return json.loads(message)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

        raise ValueError("Failed to parse JSON data")

    def parse_data(self, client_socket: socket.socket) -> Any:
        """Parse data based on the configured mode"""
        if self.data_mode == DataMode.RAW:
            return self.parse_raw_data(client_socket)
        elif self.data_mode == DataMode.STRUCTURED:
            return self.parse_structured_data(client_socket)
        elif self.data_mode == DataMode.JSON:
            return self.parse_json_data(client_socket)
        elif self.data_mode == DataMode.CUSTOM and self.custom_parser:
            return self.custom_parser(client_socket)
        else:
            raise ValueError(f"Unsupported data mode: {self.data_mode}")

    def process_data(self, data: Any, frame_count: int, client_address: tuple) -> None:
        """Process received data - override this method for custom processing"""
        if self.data_mode == DataMode.RAW:
            self.log(f"Frame {frame_count}: Received {len(data)} raw bytes from {client_address}")
            if self.verbose and len(data) > 0:
                # Show first 32 bytes as hex
                hex_data = ' '.join(f'{b:02x}' for b in data[:32])
                self.log(f"  First 32 bytes: {hex_data}")

        elif self.data_mode == DataMode.STRUCTURED:
            self.log(f"Frame {frame_count}: Received structured data from {client_address}")
            self.log(f"  Header: {data['header']}")
            self.log(f"  Data count: {data['count']}")
            if data['count'] > 0:
                self.log(f"  First few data elements: {data['data'][:min(10, len(data['data']))]}")

        elif self.data_mode == DataMode.JSON:
            self.log(f"Frame {frame_count}: Received JSON data from {client_address}")
            self.log(f"  Data: {json.dumps(data, indent=2)}")

        else:
            self.log(f"Frame {frame_count}: Received custom data from {client_address}: {data}")

    def handle_client(self, client_socket: socket.socket, client_address: tuple):
        """Handle data from a connected client"""
        self.log(f"Client connected from {client_address}")

        try:
            frame_count = 0
            while self.running:
                try:
                    data = self.parse_data(client_socket)
                    self.process_data(data, frame_count, client_address)
                    frame_count += 1

                except socket.timeout:
                    continue
                except ConnectionError:
                    self.log(f"Client {client_address} disconnected")
                    break
                except Exception as e:
                    self.log(f"Error processing frame {frame_count}: {e}")
                    if self.verbose:
                        import traceback
                        traceback.print_exc()
                    break

        except Exception as e:
            self.log(f"Error handling client {client_address}: {e}")
        finally:
            self.log(f"Client {client_address} disconnected")
            client_socket.close()

    def start(self):
        """Start the TCP server"""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            self.running = True

            self.log(f"General TCP server listening on {self.host}:{self.port}")
            self.log(f"Data mode: {self.data_mode.value}")
            self.log("Waiting for connections...")
            self.log("Press Ctrl+C to stop")

            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    client_socket.settimeout(self.timeout)

                    # Handle each client in a separate thread
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    self.client_handlers.append(client_thread)

                except socket.error as e:
                    if self.running:
                        self.log(f"Socket error: {e}")

        except KeyboardInterrupt:
            self.log("Shutting down server...")
        except Exception as e:
            self.log(f"Server error: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the TCP server"""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
            self.log("Server stopped")

def parse_radar_config(cfg_file_path):
    """Parse radar configuration file to extract timing parameters"""
    radar_config = {}
    try:
        with open(cfg_file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('% Frame Duration(msec):'):
                    frame_duration_ms = int(line.split(':')[1].strip())
                    radar_config['frame_duration_ms'] = frame_duration_ms
                    radar_config['frame_rate_hz'] = 1000.0 / frame_duration_ms
                elif line.startswith('frameCfg'):
                    # frameCfg format: frameCfg <chirp_start_idx> <chirp_end_idx> <num_loops> <num_frames> <trigger_delay> <periodicity> <trigger_select> <frame_data_format>
                    parts = line.split()
                    if len(parts) >= 7:
                        periodicity = int(parts[6])  # Frame periodicity in 5ns units
                        frame_period_ms = periodicity * 5e-6  # Convert to milliseconds
                        radar_config['frame_period_from_framecfg_ms'] = frame_period_ms
                        radar_config['calculated_frame_rate_hz'] = 1000.0 / frame_period_ms
    except Exception as e:
        print(f"Warning: Could not parse radar config {cfg_file_path}: {e}")

    return radar_config

def get_gps_timestamp(rover_data):
    """Extract GPS timestamp from rover data header"""
    if rover_data and hasattr(rover_data, 'header') and rover_data.header:
        try:
            week = int(rover_data.header.week_number)
            ms = float(rover_data.header.milliseconds)

            # GPS epoch: January 6, 1980 00:00:00 UTC
            import datetime
            GPS_EPOCH = datetime.datetime(1980, 1, 6, 0, 0, 0, tzinfo=datetime.timezone.utc)

            # Calculate GPS time
            gps_seconds = week * 604800 + ms / 1000.0

            # Convert to Unix timestamp (subtract leap seconds - currently 18)
            gps_time = GPS_EPOCH.timestamp() + gps_seconds - 18

            return gps_time
        except (ValueError, AttributeError) as e:
            print(f"Warning: Could not extract GPS timestamp: {e}")

    return None

def create_radar_parser(server_instance):
    """Create a custom parser for radar data (like the original new_tcpserver.py)"""
    def radar_parser(client_socket):
        # Receive header: numObj (uint32_t) + dummy (uint16_t)
        header_data = server_instance.receive_data(client_socket, 6)  # 4 + 2 bytes
        num_obj, dummy = struct.unpack('<IH', header_data)

        # Validate header: check if num_obj is reasonable for radar data
        if num_obj > 1000:  # Unreasonably large number of objects
            if server_instance.verbose:
                server_instance.log(f"Skipping invalid radar message: num_objects={num_obj} (too large)")
            return {"num_objects": 0, "objects": [], "skipped": True, "reason": f"num_objects {num_obj} too large"}

        if num_obj == 0:
            return {"num_objects": 0, "objects": []}

        # Receive point cloud data
        pointcloud_size = 24 * num_obj  # 24 bytes per object
        pointcloud_data = server_instance.receive_data(client_socket, pointcloud_size)

        # Parse point cloud data
        pointcloud_format = '<' + 'ffffii' * num_obj
        pointcloud = struct.unpack(pointcloud_format, pointcloud_data)

        objects = []
        for i in range(num_obj):
            obj = {
                'x': pointcloud[i*6 + 0],
                'y': pointcloud[i*6 + 1],
                'z': pointcloud[i*6 + 2],
                'velocity': pointcloud[i*6 + 3],
                'range_idx': pointcloud[i*6 + 4],
                'doppler_idx': pointcloud[i*6 + 5]
            }
            objects.append(obj)

        # Receive configuration data (following full_doppler_tcpserver.py format)
        doppler_bins_header = server_instance.receive_data(client_socket, 2)
        num_doppler_bins = struct.unpack('<H', doppler_bins_header)[0]

        # Receive number of range bins per object
        range_bins_header = server_instance.receive_data(client_socket, 2)
        num_range_bins_per_obj = struct.unpack('<H', range_bins_header)[0]

        # Receive Doppler bin data for all range bins of all objects
        doppler_data_size = num_obj * num_range_bins_per_obj * num_doppler_bins * 2  # uint16_t = 2 bytes
        doppler_data = server_instance.receive_data(client_socket, doppler_data_size)

        # Parse doppler data
        doppler_values = struct.unpack('<' + 'H' * (num_obj * num_range_bins_per_obj * num_doppler_bins), doppler_data)

        # Add range-doppler data to each object (2D matrix format matching expected output)
        for i, obj in enumerate(objects):
            range_idx = obj['range_idx']
            doppler_idx = obj['doppler_idx']

            # Calculate range bin window
            range_bin_window = (num_range_bins_per_obj - 1) // 2
            range_window_start = range_idx - range_bin_window

            # Create 2D matrix: matrix[range_bin][doppler_bin] = value
            matrix = []
            for rb in range(num_range_bins_per_obj):
                # Extract doppler values for this range bin
                doppler_start = (i * num_range_bins_per_obj + rb) * num_doppler_bins
                doppler_vals = list(doppler_values[doppler_start:doppler_start + num_doppler_bins])
                matrix.append(doppler_vals)

            # Add structured range-doppler data to object
            obj['range_doppler_slice'] = {
                'num_doppler_bins': num_doppler_bins,
                'num_range_bins': num_range_bins_per_obj,
                'detected_range_idx': range_idx,
                'detected_doppler_idx': doppler_idx,
                'range_window_start': range_window_start,
                'matrix': matrix
            }

        # Log successful parse
        if server_instance.verbose:
            server_instance.log(f"Successfully parsed radar data: {num_obj} objects, {num_doppler_bins} doppler bins, {num_range_bins_per_obj} range bins per object")

        return {"num_objects": num_obj, "objects": objects}

    return radar_parser

def main():
    """Main function to setup GPS and start TCP server"""
    parser = argparse.ArgumentParser(description='GPS Data Collection with TCP Server')

    # GPS arguments
    parser.add_argument('--base-port', default='/dev/ttyUSB1', help='Base station GPS port (default: /dev/ttyUSB0)')
    parser.add_argument('--rover-port', default='/dev/ttyUSB0', help='Rover GPS port (default: /dev/ttyUSB1)')
    parser.add_argument('--base-baud', type=int, default=115200, help='Base station baudrate (default: 115200)')
    parser.add_argument('--rover-baud', type=int, default=115200, help='Rover baudrate (default: 57600)')

    # Radar CLI arguments
    parser.add_argument('--radar-cli-port', default='/dev/ttyACM0', help='Radar CLI port (default: /dev/ttyACM0)')
    parser.add_argument('--radar-cli-baud', type=int, default=115200, help='Radar CLI baudrate (default: 115200)')

    # TCP Server arguments
    parser.add_argument('--host', default='0.0.0.0', help='Server host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=7, help='Server port (default: 7)')
    parser.add_argument('--mode', choices=['raw', 'structured', 'json', 'radar'],
                       default='radar', help='Data parsing mode (default: radar)')
    parser.add_argument('--buffer-size', type=int, default=4096,
                       help='Buffer size for raw mode (default: 4096)')
    parser.add_argument('--timeout', type=float, default=1.0,
                       help='Socket timeout in seconds (default: 1.0)')
    parser.add_argument('--header-format', default='<I',
                       help='Struct format for header in structured mode (default: <I)')
    parser.add_argument('--data-format', default='<f',
                       help='Struct format for data elements in structured mode (default: <f)')
    parser.add_argument('--quiet', action='store_true', help='Disable verbose logging')
    parser.add_argument('--skip-gps', action='store_true', help='Skip GPS setup and go straight to TCP server')

    # Data logging arguments
    parser.add_argument('--disable-logging', action='store_true', help='Disable data logging to disk')
    parser.add_argument('--log-path', default='data', help='Data logging base path (default: data)')
    parser.add_argument('--max-file-size', type=int, default=100, help='Max file size in MB (default: 100)')
    parser.add_argument('--log-buffer-size', type=int, default=100, help='Logging buffer size (default: 100)')

    args = parser.parse_args()

    # Enable logging by default unless explicitly disabled
    args.enable_logging = not args.disable_logging

    # Print startup assumptions
    print("🚀 GPS Data Collection with TCP Server")
    print("="*60)
    print("📋 SYSTEM ASSUMPTIONS & PORT CONFIGURATION:")
    print(f"   📍 Base Station GPS     : {args.base_port} @ {args.base_baud} baud")
    print(f"   📡 Rover GPS           : {args.rover_port} @ {args.rover_baud} baud")
    print(f"   🎯 Radar CLI           : {args.radar_cli_port} @ {args.radar_cli_baud} baud")
    print(f"   🌐 TCP Server          : {args.host}:{args.port}")
    print(f"   📊 Data Mode           : {args.mode}")
    if args.skip_gps:
        print("   ⚠️  GPS Setup          : SKIPPED")
    else:
        print("   ✅ GPS Setup           : ENABLED")
    if args.enable_logging:
        print(f"   💾 Data Logging        : ENABLED → {args.log_path}")
        print(f"   📁 Max File Size       : {args.max_file_size} MB")
        print(f"   🔄 Buffer Size         : {args.log_buffer_size} records")
    else:
        print("   ⚠️  Data Logging       : DISABLED")
    print("="*60)
    print()

    # Initialize DataLogger if enabled
    data_logger = None
    if args.enable_logging:
        print("🔧 Initializing data logger...")
        data_logger = DataLogger(
            base_path=Path(args.log_path),
            max_file_size_mb=args.max_file_size,
            buffer_size=args.log_buffer_size
        )
        print(f"   📁 Session: {data_logger.get_session_path()}")
        print("   ✅ Data logger ready")
        print()

    base_coords = None
    rover_reader = None

    if not args.skip_gps:
        print("🔧 INITIALIZING GPS SYSTEM:")
        print("📍 Setting up base station...")
        base_coords = get_base_station_coords(args.base_port, args.base_baud)

        if not base_coords:
            print("❌ Failed to get base station coordinates. Exiting...")
            return

        print(f"✅ Base station setup complete: ({base_coords.lat:.6f}, {base_coords.lon:.6f})")

        # Step 2: Start rover reader
        print("📡 Starting rover GPS reader...")
        rover_reader = UM980Reader(port=args.rover_port, baudrate=args.rover_baud, debug_label="ROVER")

        try:
            rover_reader.start()
            print("✅ Rover GPS reader started")

            # Wait for rover to get initial position
            print("⏳ Waiting for rover initial position...")
            for i in range(10):
                rover_data = rover_reader.get_data()
                if rover_data.lat is not None:
                    enu_coords = convert_gps_to_enu_cm(base_coords, rover_data)
                    print(f"✅ Rover initial position (ENU cm): {enu_coords}")
                    break
                time.sleep(1)
            else:
                print("⚠️ Rover position not acquired, continuing anyway...")

        except Exception as e:
            print(f"❌ Failed to start rover reader: {e}")
            return

    # Step 3: Setup and start TCP server
    print("🔧 INITIALIZING TCP SERVER:")

    # Convert mode string to enum
    mode_map = {
        'raw': DataMode.RAW,
        'structured': DataMode.STRUCTURED,
        'json': DataMode.JSON,
        'radar': DataMode.CUSTOM
    }

    data_mode = mode_map[args.mode]

    # Parse radar configuration
    CFG_PATH = Path("tdm_enet.cfg")
    radar_timing_config = parse_radar_config(CFG_PATH)
    if radar_timing_config:
        print(f"📡 Radar timing configuration parsed:")
        print(f"   Frame rate: {radar_timing_config.get('frame_rate_hz', 'unknown')} Hz")
        print(f"   Frame duration: {radar_timing_config.get('frame_duration_ms', 'unknown')} ms")

    # Create custom TCP server that integrates with GPS data
    class GPSTCPServer(GeneralTCPServer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.base_coords = base_coords
            self.rover_reader = rover_reader
            self.data_logger = data_logger
            self.radar_config = radar_timing_config
            self.first_frame_time = None
            self.frame_start_gps_time = None
            self.gps_system_offset = 0

        def estimate_frame_capture_time(self, frame_count):
            """Estimate when this radar frame was actually captured using GPS-synchronized system time"""
            current_system_time = time.time()

            # Initialize timing on first frame using base station GPS time
            if self.first_frame_time is None:
                self.first_frame_time = current_system_time

                # Get GPS time from base station for synchronization
                if self.base_coords and hasattr(self.base_coords, 'header'):
                    base_gps_time = get_gps_timestamp(self.base_coords)
                    if base_gps_time:
                        # Calculate offset between GPS time and system time
                        self.gps_system_offset = base_gps_time - current_system_time
                        self.frame_start_gps_time = base_gps_time
                        return base_gps_time

                # Fallback if no GPS time available
                self.gps_system_offset = 0
                self.frame_start_gps_time = current_system_time
                return current_system_time

            # Calculate frame capture time using radar frame rate and GPS-synchronized time
            if self.radar_config and 'frame_rate_hz' in self.radar_config:
                frame_period = 1.0 / self.radar_config['frame_rate_hz']
                estimated_capture_time = self.frame_start_gps_time + (frame_count * frame_period)
                return estimated_capture_time

            # Fallback: use GPS-synchronized system time
            return current_system_time + self.gps_system_offset

        def process_data(self, data: Any, frame_count: int, client_address: tuple) -> None:
            """Enhanced process_data that includes GPS and radar data logging"""
            super().process_data(data, frame_count, client_address)

            # Estimate when this frame was captured
            frame_capture_timestamp = self.estimate_frame_capture_time(frame_count)

            # Log radar data if DataLogger is enabled and we have radar data (dict with objects)
            if self.data_logger and isinstance(data, dict) and "objects" in data:
                self.log(f"DEBUG: Logging radar data - {data.get('num_objects', 0)} objects")

                # Store radar timing configuration in session metadata on first frame
                if frame_count == 0 and self.radar_config:
                    timing_metadata = {
                        "radar_timing": self.radar_config,
                        "timestamp_method": "gps_synchronized_frame_estimation",
                        "gps_leap_seconds_offset": 18,
                        "frame_timing_note": "Timestamps estimated using GPS time + frame_count * frame_period"
                    }
                    self.data_logger.add_metadata("radar_timing_configuration", timing_metadata)

                # Store radar configuration in session metadata if available
                if data.get("objects") and len(data["objects"]) > 0:
                    first_obj = data["objects"][0]
                    if "range_doppler_slice" in first_obj:
                        slice_data = first_obj["range_doppler_slice"]
                        radar_config = {
                            "sensor_type": "AWR2944PEVM",
                            "num_doppler_bins": slice_data.get("num_doppler_bins", 16),
                            "num_range_bins_per_object": slice_data.get("num_range_bins", 31),
                            "range_bin_window_size": slice_data.get("num_range_bins", 31),
                            "detection_window_type": "centered"
                        }

                        # Add radar configuration to metadata
                        self.data_logger.add_metadata("radar_configuration", radar_config)

                radar_event = {
                    "timestamp": frame_capture_timestamp,
                    "system_timestamp": time.time(),
                    "frame": frame_count,
                    "client": f"{client_address[0]}:{client_address[1]}",
                    "num_objects": data.get("num_objects", 0),
                    "objects": data.get("objects", [])
                }
                try:
                    # Convert radar data to bytes for raw storage
                    radar_raw_bytes = json.dumps(data).encode('utf-8')
                    self.data_logger.write_event("radar", frame_capture_timestamp, radar_event, raw_bytes=radar_raw_bytes)
                    self.log(f"DEBUG: Successfully logged radar event for frame {frame_count}")
                except Exception as e:
                    self.log(f"ERROR: Failed to write radar event to logger: {e}")
                    import traceback
                    self.log(f"Traceback: {traceback.format_exc()}")

            # Add GPS data to logging if available
            if self.rover_reader and self.base_coords:
                try:
                    rover_data = self.rover_reader.get_data()
                    if rover_data.lat is not None:
                        enu_coords = convert_gps_to_enu_cm(self.base_coords, rover_data)
                        self.log(f"  GPS Position (ENU cm): X={enu_coords['x_cm']:.1f}, Y={enu_coords['y_cm']:.1f}, Z={enu_coords['z_cm']:.1f}")
                        self.log(f"  GPS Position type: {rover_data.pos_type}")

                        # Log GPS data to DataLogger if enabled
                        if self.data_logger:
                            gps_event = {
                                "timestamp": get_gps_timestamp(rover_data) or time.time(),
                                "frame": frame_count,
                                "client": f"{client_address[0]}:{client_address[1]}",
                                "gps_raw": {
                                    "latitude": float(rover_data.lat),
                                    "longitude": float(rover_data.lon),
                                    "altitude": float(rover_data.height),
                                    "position_type": str(rover_data.pos_type)
                                },
                                "gps_enu_cm": {
                                    "x_cm": float(enu_coords['x_cm']),
                                    "y_cm": float(enu_coords['y_cm']),
                                    "z_cm": float(enu_coords['z_cm'])
                                }
                            }
                            self.data_logger.write_event("gps", time.time(), gps_event)

                except Exception as e:
                    self.log(f"  GPS data error: {e}")

    global server
    server = GPSTCPServer(
        host=args.host,
        port=args.port,
        data_mode=data_mode,
        buffer_size=args.buffer_size,
        timeout=args.timeout,
        header_format=args.header_format,
        data_format=args.data_format,
        custom_parser=None,
        verbose=not args.quiet
    )

    # Set custom parser after server is created (for radar mode)
    if args.mode == 'radar':
        server.custom_parser = create_radar_parser(server)

    print("🔥 STARTING TCP SERVER WITH GPS INTEGRATION...")
    print("="*60)

    # Start radar configuration and monitoring
    print("🎯 STARTING RADAR CONFIGURATION...")
    print("="*60)
    try:
        CFG_PATH = Path("tdm_enet.cfg")

        def minimal_radar_config(port, cfg_file, baud=115200):
            """Minimal 20% radar config that does 80% of the work"""
            ser = serial.Serial(port, baud, timeout=0.1)
            time.sleep(0.2)
            ser.reset_input_buffer()
            ser.reset_output_buffer()

            # Stop any running sensor
            ser.write(b"sensorStop\n")
            ser.readline()
            ser.write(b"flushCfg\n")
            ser.readline()

            # Send config
            with open(cfg_file, 'r') as f:
                for i, line in enumerate(f, 1):
                    line = line.strip()
                    if line and not line.startswith(('#', '%', '!', '//')):
                        print(f"[{i}] {line}")
                        ser.write(line.encode() + b'\n')
                        ser.flush()
                        time.sleep(0.1)  # Give radar time to process
                        response = ser.readline()
                        print(response.decode(errors='ignore').strip())

            # Monitor output for 5 seconds then exit
            print("="*60)
            print("CONFIG SENT - Monitoring for 5 seconds...")
            print("="*60)
            try:
                start_monitoring = time.time()
                while time.time() - start_monitoring < 5.0:  # Monitor for 5 seconds only
                    if ser.in_waiting > 0:
                        data = ser.read(ser.in_waiting).decode(errors='ignore')
                        for line in data.split('\n'):
                            if line.strip():
                                print(f"[RADAR] {line}")
                    else:
                        time.sleep(0.1)
                print("[RADAR] Monitoring complete - radar configured and running")
            except KeyboardInterrupt:
                print("Monitoring stopped")
            finally:
                ser.close()

        radar_thread = threading.Thread(target=minimal_radar_config, args=(args.radar_cli_port, CFG_PATH, args.radar_cli_baud), daemon=True)
        radar_thread.start()
        print("✅ Minimal radar configuration thread started")
    except Exception as e:
        print(f"⚠️ Warning: Could not start radar configuration: {e}")
        print("   Continuing with TCP server only...")

    try:
        server.start()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    finally:
        if data_logger:
            print("💾 Closing data logger...")
            data_logger.close()
            print(f"   📁 Data saved to: {data_logger.get_session_path()}")
        if rover_reader:
            print("🔌 Stopping rover GPS reader...")
            rover_reader.stop()
        print("👋 Goodbye!")

# --- Data Logging System ---
class DataLogger:
    """Unified high-performance data logger for GPS and sensor data"""

    EVENT_TYPES = {
        "gps": 1,
        "radar": 2,
        "system": 3,
        "worker": 4,
        "large": 5,
        "test": 6,
        "shutdown_test": 7
    }

    def __init__(self, base_path: Optional[Path] = None, max_file_size_mb: int = 1024,
                 buffer_size: int = 100, flush_interval_ms: int = 100):
        """
        Initialize DataLogger

        Args:
            base_path: Base directory for data storage (default: ./data)
            max_file_size_mb: Maximum file size before rolling to new file
            buffer_size: Maximum number of records to buffer before force flush
            flush_interval_ms: Automatic flush interval in milliseconds
        """
        self.base_path = Path(base_path) if base_path else Path("./data")
        self.max_file_size_bytes = int(max_file_size_mb * 1024 * 1024)
        self.buffer_size = buffer_size
        self.flush_interval_ms = flush_interval_ms

        # Create session folder with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_path = self.base_path / f"session_{timestamp}"
        self.session_path.mkdir(parents=True, exist_ok=True)

        # Initialize file management
        self.current_file_index = 0
        self.current_file = None
        self.current_file_size = 0

        # Initialize buffering system
        self.write_queue = queue.Queue()
        self.writer_thread = None
        self.running = False

        # Thread-safe file operations lock
        self.file_lock = threading.Lock()

        # Create metadata file
        self._create_metadata()

        # Open first data file
        self._open_new_file()

        # Start writer thread
        self._start_writer_thread()

    def get_session_path(self) -> Path:
        """Get the current session path"""
        return self.session_path

    def write_event(self, event_type: str, timestamp: float, data: Dict[str, Any], raw_bytes: Optional[bytes] = None):
        """
        Write an event to the buffer (thread-safe)

        Args:
            event_type: Type of event ("gps", "radar", "system", etc.)
            timestamp: Unix timestamp (seconds since epoch)
            data: Dictionary of structured data
            raw_bytes: Optional raw binary data
        """
        # Get event type ID
        event_type_id = self.EVENT_TYPES.get(event_type, 99)  # 99 for unknown

        # Serialize data to JSON
        json_data = json.dumps(data).encode('utf-8')
        data_size = len(json_data)

        # Prepare raw bytes
        if raw_bytes is None:
            raw_bytes = b''
        raw_size = len(raw_bytes)

        # Pack binary record
        # Format: timestamp(8) + event_type_id(4) + data_size(4) + json_data + raw_size(4) + raw_bytes
        record = struct.pack('<dII', timestamp, event_type_id, data_size)
        record += json_data
        record += struct.pack('<I', raw_size)
        record += raw_bytes

        # Add to write queue (non-blocking)
        try:
            self.write_queue.put_nowait(record)

            # Force flush if buffer is getting full (overflow protection)
            if self.write_queue.qsize() >= self.buffer_size:
                self._force_flush()

        except queue.Full:
            # Emergency: write directly to disk if queue is full
            self._write_record_directly(record)

    def _create_metadata(self):
        """Create session metadata file"""
        metadata = {
            "session_id": self.session_path.name,
            "start_time": datetime.now().isoformat(),
            "data_format_version": "1.0",
            "event_types": {name: id for name, id in self.EVENT_TYPES.items()},
            "record_format": {
                "timestamp": "double (8 bytes)",
                "event_type_id": "uint32 (4 bytes)",
                "data_size": "uint32 (4 bytes)",
                "json_data": "variable length UTF-8",
                "raw_size": "uint32 (4 bytes)",
                "raw_bytes": "variable length binary"
            }
        }

        metadata_file = self.session_path / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

    def add_metadata(self, key: str, value: Any):
        """Add additional metadata to the session metadata file"""
        metadata_file = self.session_path / "metadata.json"

        # Read existing metadata
        try:
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            metadata = {}

        # Add new metadata
        metadata[key] = value

        # Write back to file
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

    def _open_new_file(self):
        """Open new data file"""
        if self.current_file:
            self.current_file.close()

        file_path = self.session_path / f"data_{self.current_file_index:03d}.bin"
        self.current_file = open(file_path, 'wb')
        self.current_file_size = 0

    def _roll_to_new_file(self):
        """Roll to new data file when current file gets too large"""
        self.current_file_index += 1
        self._open_new_file()

    def _start_writer_thread(self):
        """Start the background writer thread"""
        self.running = True
        self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self.writer_thread.start()

    def _writer_loop(self):
        """Background thread that periodically flushes the write queue"""
        last_flush_time = time.time()

        while self.running:
            current_time = time.time()
            should_flush = False

            # Check if it's time for periodic flush
            if (current_time - last_flush_time) * 1000 >= self.flush_interval_ms:
                should_flush = True

            # Check if buffer has records
            if not self.write_queue.empty():
                should_flush = True

            if should_flush:
                self._flush_queue()
                last_flush_time = current_time

            # Sleep briefly to avoid busy waiting
            time.sleep(0.01)  # 10ms

    def _flush_queue(self):
        """Flush all pending records from queue to disk"""
        records_written = 0

        while not self.write_queue.empty():
            try:
                record = self.write_queue.get_nowait()
                self._write_record_directly(record)
                records_written += 1
            except queue.Empty:
                break

        # Ensure data is written to disk
        if records_written > 0:
            with self.file_lock:
                if self.current_file:
                    self.current_file.flush()

    def _write_record_directly(self, record: bytes):
        """Write a single record directly to the current file"""
        record_size = len(record)

        # Thread-safe file operations: protect entire check-roll-write-update sequence
        with self.file_lock:
            # Check if we need to roll to new file
            if self.current_file_size + record_size >= self.max_file_size_bytes:
                self._roll_to_new_file()

            # Write to file
            if self.current_file:
                self.current_file.write(record)
                self.current_file_size += record_size

    def _force_flush(self):
        """Force immediate flush of the write queue"""
        self._flush_queue()

    def close(self):
        """Close the data logger and flush all data"""
        # Stop writer thread
        self.running = False
        if self.writer_thread and self.writer_thread.is_alive():
            self.writer_thread.join(timeout=1.0)

        # Flush any remaining data
        self._flush_queue()

        # Close file (thread-safe)
        with self.file_lock:
            if self.current_file:
                self.current_file.close()
                self.current_file = None

    def _flush_buffer(self):
        """Flush any buffered data (for testing)"""
        self._flush_queue()
        with self.file_lock:
            if self.current_file:
                self.current_file.flush()

if __name__ == "__main__":
    main()
