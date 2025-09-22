from start_data_collection import UM980Reader
import time

def main():
    """Test GPS readers."""
    g1 = UM980Reader("/dev/ttyUSB0", 115200)
    g1.start()
    g2 = UM980Reader("/dev/ttyUSB1", 115200)
    g2.start()

    while True:
        time.sleep(0.05)
        data1 = g1.get_data()
        data2 = g2.get_data()
        print(f"GPS1: lon: {data1.lon}, lat:  {data1.lat}, type: {data1.pos_type}")
        print(f"GPS2: lon: {data2.lon}, lat:  {data2.lat}, type: {data2.pos_type}")

if __name__ == "__main__":
    main()
