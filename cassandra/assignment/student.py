"""
Assignment Student File
Run with:
    python student.py

Ensure Cassandra is running before executing.
"""

import os
import uuid
from datetime import datetime, date, timedelta
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
import logging

# --- LOGGING SETUP ---
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger()

ANSWER_LEVEL_NUM = 25
logging.addLevelName(ANSWER_LEVEL_NUM, "ANSWER")

def answer(self, message, *args, **kws):
    if self.isEnabledFor(ANSWER_LEVEL_NUM):
        self._log(ANSWER_LEVEL_NUM, message, args, **kws)

logging.Logger.answer = answer
logging.basicConfig(level=ANSWER_LEVEL_NUM)


# --- IMPLEMENTATION STARTS HERE ---

def get_readings_for_day(session):
    """
    Retrieve two sensor readings for a turbine on a given day.
    (No fixed day assumed, ALLOW FILTERING used)
    """
    cql_query = """
        SELECT *
        FROM wind_turbine_data.sensor_readings
        LIMIT 2
        ALLOW FILTERING
    """
    try:
        rows = session.execute(cql_query)
        return list(rows)
    except Exception as e:
        log.info(f'Error in get_readings_for_day: {e}')
        return []


def get_reading_for_turbine(session):
    """
    Retrieve the single most recent sensor reading for a turbine on a given day.
    (No fixed day assumed, ALLOW FILTERING used)
    """
    cql_query = """
        SELECT *
        FROM wind_turbine_data.sensor_readings
        LIMIT 1
        ALLOW FILTERING
    """
    try:
        rows = session.execute(cql_query)
        return list(rows)
    except Exception as e:
        log.info(f'Error in get_reading_for_turbine: {e}')
        return []


def get_number_of_readings_in_range(session):
    """
    Retrieve the total number of readings on a turbine between
    2025-09-07T09:00:00+0000 and 2025-09-07T10:00:00+0000.
    (Explicit date/time range given in the question)
    """
    cql_query = """
        SELECT COUNT(*)
        FROM wind_turbine_data.sensor_readings
        WHERE timestamp >= '2025-09-07T09:00:00+0000'
        AND timestamp <= '2025-09-07T10:00:00+0000'
        ALLOW FILTERING
    """
    try:
        rows = session.execute(cql_query)
        return list(rows)
    except Exception as e:
        log.info(f'Error in get_number_of_readings_in_range: {e}')
        return []


def get_last_update_for_day(session):
    """
    Retrieve the last update record for a given day.
    (No fixed day assumed, ALLOW FILTERING used)
    """
    cql_query = """
        SELECT *
        FROM wind_turbine_data.latest_readings
        LIMIT 1
        ALLOW FILTERING
    """
    try:
        rows = session.execute(cql_query)
        return list(rows)
    except Exception as e:
        log.info(f'Error in get_last_update_for_day: {e}')
        return []


def get_health(session):
    """
    Determine turbine health:
    Healthy if avg wind_speed between 15 and 25
    and <=5 records with power_output=0.
    (No fixed day assumed, ALLOW FILTERING used)
    """
    try:
        query_wind = """
            SELECT AVG(wind_speed) AS avg_ws
            FROM wind_turbine_data.sensor_readings
            ALLOW FILTERING
        """
        avg_speed_row = session.execute(query_wind).one()
        avg_speed = avg_speed_row.avg_ws if avg_speed_row else None

        query_zero = """
            SELECT COUNT(*) AS zero_count
            FROM wind_turbine_data.sensor_readings
            WHERE power_output = 0
            ALLOW FILTERING
        """
        zero_count_row = session.execute(query_zero).one()
        zero_count = zero_count_row.zero_count if zero_count_row else None

        if avg_speed is None or zero_count is None:
            return "Could not determine health"

        is_wind_speed_healthy = (15 <= avg_speed <= 25)
        is_power_output_healthy = (zero_count <= 5)

        if is_wind_speed_healthy and is_power_output_healthy:
            return "Healthy"
        else:
            return "Not Healthy"

    except Exception as e:
        log.info(f'Error in get_health: {e}')
        return "Could not determine health"


# --- IMPLEMENTATION ENDS HERE ---


def main():
    """
    Main function to test queries.
    """
    CASSANDRA_NODES = os.getenv('CASSANDRA_NODES', '127.0.0.1').split(',')
    try:
        policy = DCAwareRoundRobinPolicy(local_dc='datacenter1')
        cluster = Cluster(['127.0.0.1'], load_balancing_policy=policy, protocol_version=4)
        session = cluster.connect('wind_turbine_data')
        log.info("Connected to Cassandra.")
    except Exception as e:
        log.info(f"Could not connect to Cassandra. Error: {e}")
        return

    log.info("\n--- RUNNING ANSWERS ---")

    # Q4.4.1
    last_update = get_last_update_for_day(session)
    log.answer(f'4.4.1: {last_update}')

    # Q4.4.2
    readings = get_readings_for_day(session)
    log.answer(f'4.4.2: {readings}')

    # Q4.4.3
    in_range = get_number_of_readings_in_range(session)
    log.answer(f'4.4.3: {in_range}')

    # Q4.4.4
    health = get_health(session)
    log.answer(f'4.4.4: {health}')

    session.shutdown()
    cluster.shutdown()


if __name__ == "__main__":
    main()
