#!/home/pi/pysma/bin/python3

import argparse
import asyncio
import logging
import signal
import sys
import pickle
import time

import aiohttp
import sqlite3
import pysma

_LOGGER = logging.getLogger(__name__)

VAR = {}

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        #print(sqlite3.version)
    except sqlite3.Error as e:
        print(e)

    return conn

def write_db(sensors):
    conn = create_connection('/home/pi/pysma/pysma.db')

    sensor_names = ['dateTime']
    sensor_values = [int(time.time())]

    for sensor in sensors:
        sensor_names.append(sensor.name)
        sensor_values.append(sensor.value)

    #print(sensor_names)
    #print(sensor_values)

    sql = ''' INSERT INTO archive(dateTime, status, pv_power_a, pv_power_b, pv_power_c, pv_voltage_a,
            pv_voltage_b, pv_voltage_c, pv_current_a, pv_current_b, pv_current_c, grid_power,
            frequency, current_l1, current_l2, current_l3, voltage_l1, voltage_l2, voltage_l3,
            power_l1, power_l2, power_l3, total_yield, daily_yield, pv_gen_meter,
            metering_power_supplied, metering_power_absorbed, metering_frequency,
            metering_total_yield, metering_total_absorbed, metering_current_l1,
            metering_current_l2, metering_current_l3, metering_voltage_l1, metering_voltage_l2,
            metering_voltage_l3, metering_active_power_feed_l1, metering_active_power_feed_l2,
            metering_active_power_feed_l3, metering_active_power_draw_l1,
            metering_active_power_draw_l2, metering_active_power_draw_l3,
            metering_current_consumption, metering_total_consumption)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ) '''

    if len(sensor_values) < 40:
        print("Inverter not returning full sensor list. Sleeping?")
        print(sensor_names)
        return None
    else:
        cur = conn.cursor()
        cur.execute(sql, sensor_values)
        conn.commit()


def print_table(sensors):
    """Print sensors formatted as table."""
    for sen in sensors:
        if sen.value is None:
            print("{:>25}".format(sen.name))
        else:
            print("{:>25}{:>15} {}".format(sen.name, str(sen.value), sen.unit))


async def main_loop(password, user, url):
    """Run main loop."""
    sensors = None

    async with aiohttp.ClientSession(
        connector=aiohttp.TCPConnector(ssl=False)
    ) as session:
        VAR["sma"] = pysma.SMA(session, url, password=password, group=user)

        try:
            await VAR["sma"].new_session()
        except pysma.exceptions.SmaAuthenticationException:
            _LOGGER.warning("Authentication failed!")
            return
        except pysma.exceptions.SmaConnectionException:
            _LOGGER.warning("Unable to connect to device at %s", url)
            return

        # We should not get any exceptions, but if we do we will close the session.
        try:
            VAR["running"] = True
            cnt = 1
            sensors = await VAR["sma"].get_sensors()
            device_info = await VAR["sma"].device_info()

            #for name, value in device_info.items():
            #    print("{:>15}{:>25}".format(name, value))

            # enable all sensors
            for sensor in sensors:
                sensor.enabled = True

            while VAR.get("running"):
                await VAR["sma"].read(sensors)
                #print_table(sensors)
                cnt -= 1
                if cnt == 0:
                    break
                await asyncio.sleep(2)

        finally:
            _LOGGER.info("Closing Session...")
            await VAR["sma"].close_session()
            write_db(sensors)





async def main():
    logging.basicConfig(stream=sys.stdout, level=logging.WARNING)

    parser = argparse.ArgumentParser(description="Test the SMA webconnect library.")
    parser.add_argument(
        "url",
        type=str,
        help="Web address of the Webconnect module (http://ip-address or https://ip-address)",
    )
    parser.add_argument("user", choices=["user", "installer"], help="Login username")
    parser.add_argument("password", help="Login password")

    args = parser.parse_args()

    def _shutdown(*_):
        VAR["running"] = False

    signal.signal(signal.SIGINT, _shutdown)

    await main_loop(user=args.user, password=args.password, url=args.url)


if __name__ == "__main__":
    asyncio.run(main())
