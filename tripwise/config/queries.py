"""
Query Configuration for Tripwise App
Uses serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers and serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies tables
"""

import os


class QueryConfig:
    """Configuration for SQL queries and warehouse settings"""

    def __init__(self):
        self.warehouse_id = os.environ.get('DATABRICKS_WAREHOUSE_ID', '22ad910fe1aed92d')

    def get_warehouse_id(self):
        return self.warehouse_id

    def get_drivers_sql(self):
        """Query to fetch active drivers from serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers"""
        return """
        SELECT
            device_id,
            driver_name,
            vehicle_info,
            current_driving_score,
            total_trips,
            total_miles,
            risk_tier,
            status
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers
        WHERE status = 'active'
        ORDER BY current_driving_score DESC
        LIMIT 100
        """

    def get_driver_details_sql(self, device_id):
        """Query to fetch driver with policy details"""
        return f"""
        SELECT
            d.device_id,
            d.driver_name,
            d.vehicle_info,
            d.policy_number,
            d.registration_date,
            d.current_driving_score,
            d.total_trips,
            d.total_miles,
            d.risk_tier,
            d.status,
            p.policy_id,
            p.premium_amount,
            p.premium_period,
            p.discount_pct,
            p.coverage_level,
            p.effective_date,
            p.is_active as policy_is_active
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers d
        LEFT JOIN serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies p
            ON d.device_id = p.device_id AND p.is_active = true
        WHERE d.device_id = '{device_id}'
        and p.policy_type != 'embedded_trip'
        LIMIT 1
        """

    def get_update_policy_sql(self, device_id, policy_id, new_premium, new_discount):
        """Query to update premium and discount for a policy"""
        update_parts = []
        if new_premium is not None:
            update_parts.append(f"premium_amount = {float(new_premium)}")
        if new_discount is not None:
            update_parts.append(f"discount_pct = {float(new_discount)}")

        update_clause = ", ".join(update_parts)

        if policy_id:
            return f"""
            UPDATE serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies
            SET {update_clause}
            WHERE policy_id = '{policy_id}'
            """
        else:
            return f"""
            UPDATE serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies
            SET {update_clause}
            WHERE device_id = '{device_id}' AND is_active = true
            """

    def get_lakebase_devices_sql(self):
        """Query to fetch devices from Lakebase (synced to Unity Catalog)"""
        return """
        SELECT
            device_id,
            driver_name,
            registered_at,
            registration_latitude,
            registration_longitude
        FROM databricks_postgres.tripwise_demo.devices
        ORDER BY registered_at DESC
        """

    def get_agg_location_sql(self, state):
        """Query to lookup top converting trigger for a state"""
        return f"""
        SELECT converting_trigger
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.agg_location
        WHERE state = '{state}'
        LIMIT 1
        """
