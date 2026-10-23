"""
Tripwise Insurance Agent Portal
- Custom Analytics Dashboard with KPIs, charts, maps, funnel, Sankey
- Embedded Dashboard for detailed analytics
- Genie AI Assistant with chat functionality
- Agent actions: adjust premiums/discounts and send personalized emails
"""

from flask import Flask, render_template, request, jsonify
from genie_embedding import (
    new_genie_conversation,
    get_genie_space_id,
    get_databricks_oauth_token,
    continue_genie_conversation
)
from dotenv import load_dotenv
import os
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from config.queries import QueryConfig
import psycopg2

load_dotenv()

app = Flask(__name__, static_folder='static', static_url_path='/static')
logging.basicConfig(level=logging.INFO)
query_config = QueryConfig()

databricks_host = os.environ.get('DATABRICKS_HOST', 'https://fevm-serverless-stable-mv5gxq.cloud.databricks.com')
databricks_host = databricks_host.rstrip('/')

w = WorkspaceClient()

PREMIUM_FORECAST_ENDPOINT = os.environ.get('PREMIUM_FORECAST_ENDPOINT', 'premium_forecast_tripwise')

LAKEBASE_HOST = os.environ.get('LAKEBASE_HOST', 'ep-soft-frost-d263qfy8.database.us-east-1.cloud.databricks.com')
LAKEBASE_USER = os.environ.get('LAKEBASE_USER', 'faee39eb-c8e5-4d99-ada7-4deca1cea3e4')
LAKEBASE_INSTANCE_NAME = os.environ.get('LAKEBASE_INSTANCE_NAME', 'tripwise')
LAKEBASE_SCHEMA = os.environ.get('LAKEBASE_SCHEMA', 'tripwise_demo')
LAKEBASE_ENDPOINT = os.environ.get(
    'LAKEBASE_ENDPOINT',
    'projects/tripwise/branches/production/endpoints/primary'
)


def get_lakebase_connection():
    """Get a direct PostgreSQL connection to Lakebase (autoscaling tier) using an OAuth token."""
    cred = w.api_client.do(
        'POST', '/api/2.0/postgres/credentials',
        body={'endpoint': LAKEBASE_ENDPOINT}
    )
    return psycopg2.connect(
        host=LAKEBASE_HOST,
        port=5432,
        dbname='databricks_postgres',
        user=LAKEBASE_USER,
        password=cred['token'],
        sslmode='require',
        options=f'-c search_path={LAKEBASE_SCHEMA}'
    )


TRIGGER_TO_PRODUCT = {
    "leaving_home_zone":       {"name": "Extended Zone Protection", "price": "$2.49/trip", "id": "prod-101"},
    "bad_neighborhood":        {"name": "High-Risk Zone Shield",  "price": "$1.79/trip", "id": "prod-102"},
    "inclement_weather":       {"name": "Adverse Weather Cover",  "price": "$0.99/trip", "id": "prod-103"},
    "rough_road_conditions":   {"name": "Roadside On-Demand",     "price": "$1.29/trip", "id": "prod-104"},
}


# ============================================================================
# ROUTES
# ============================================================================

@app.route('/')
@app.route('/analytics')
def analytics():
    dashboard_id = os.environ.get('DATABRICKS_DASHBOARD_ID', '01f15bdebe6a12d8b153f14e54703b8e')
    dashboard_url = f"{databricks_host}/embed/dashboardsv3/{dashboard_id}?theme=dark"
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username', 'Agent')
    return render_template('analytics.html',
                           dashboard_url=dashboard_url,
                           user=extract_name(forwarded_user),
                           first_name=extract_first_name(forwarded_user))


@app.route('/insights')
def insights():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username', 'Agent')
    return render_template('analytics_v2.html',
                           user=extract_name(forwarded_user),
                           first_name=extract_first_name(forwarded_user))


@app.route('/genie')
def genie():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username', 'Agent')
    return render_template('genie.html',
                           user=extract_name(forwarded_user),
                           first_name=extract_first_name(forwarded_user))


@app.route('/agent')
def agent_actions():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username', 'Agent')
    return render_template('agent.html',
                           user=extract_name(forwarded_user),
                           first_name=extract_first_name(forwarded_user))


@app.route('/offer')
def offer_actions():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username', 'Agent')
    return render_template('offer.html',
                           user=extract_name(forwarded_user),
                           first_name=extract_first_name(forwarded_user))


@app.route('/registrations')
def registrations():
    forwarded_user = request.headers.get('X-Forwarded-Preferred-Username', 'Agent')
    return render_template('registrations.html',
                           user=extract_name(forwarded_user),
                           first_name=extract_first_name(forwarded_user))


# ============================================================================
# ANALYTICS API ENDPOINTS
# (Queries sourced from dashboard_queries.md)
# ============================================================================

@app.route('/api/analytics/kpis')
def analytics_kpis():
    """Hero KPIs: total revenue, active policies, conversion rate, embedded %.
    Uses dashboard queries: policies table + fact_offers + embedded revenue % CTE."""
    try:
        # Revenue + active policies from policies table
        rev_sql = """
        SELECT
            SUM(premium_amount) AS total_revenue,
            SUM(CASE WHEN is_active = true THEN 1 ELSE 0 END) AS active_policies,
            SUM(CASE WHEN premium_period = 'trip' THEN premium_amount ELSE 0 END) AS embedded_revenue,
            SUM(CASE WHEN premium_period = 'annual' THEN premium_amount ELSE 0 END) AS full_term_revenue
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies
        """
        # Conversion rate from fact_offers
        conv_sql = """
        SELECT
            COUNT(*) AS total_offers,
            SUM(CASE WHEN was_clicked THEN 1 ELSE 0 END) AS clicked,
            SUM(CASE WHEN did_sign_up THEN 1 ELSE 0 END) AS converted
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.fact_offers
        """
        rev_result = execute_sql_query(rev_sql)
        conv_result = execute_sql_query(conv_sql)

        if rev_result["status"] != "success" or conv_result["status"] != "success":
            return jsonify({"status": "error", "message": "Query failed"}), 500

        rev_row = rev_result["data"][0]
        conv_row = conv_result["data"][0]

        total_revenue = float(rev_row[0] or 0)
        active_policies = int(rev_row[1] or 0)
        embedded_revenue = float(rev_row[2] or 0)
        full_term_revenue = float(rev_row[3] or 0)
        total_offers = int(conv_row[0] or 0)
        converted = int(conv_row[2] or 0)

        conversion_rate = (converted / total_offers * 100) if total_offers > 0 else 0
        embedded_pct = (embedded_revenue / total_revenue * 100) if total_revenue > 0 else 0

        return jsonify({
            "status": "success",
            "data": {
                "total_revenue": total_revenue,
                "active_policies": active_policies,
                "conversion_rate": round(conversion_rate, 1),
                "embedded_pct": round(embedded_pct, 1),
                "embedded_revenue": embedded_revenue,
                "full_term_revenue": full_term_revenue
            }
        })
    except Exception as e:
        logging.error(f"KPI error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/analytics/growth')
def analytics_growth():
    """Per-year policy count and revenue by policy_type.
    Derived from dashboard query: select * from policies."""
    try:
        sql = """
        SELECT
            YEAR(effective_date) AS policy_year,
            policy_type,
            COUNT(*) AS policy_count,
            SUM(premium_amount) AS total_premium
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies
        WHERE effective_date IS NOT NULL
        GROUP BY YEAR(effective_date), policy_type
        ORDER BY policy_year, policy_type
        """
        result = execute_sql_query(sql)
        if result["status"] != "success":
            return jsonify({"status": "error", "message": "Query failed"}), 500

        rows = []
        for r in result["data"]:
            rows.append({
                "year": int(r[0]),
                "policy_type": r[1],
                "policy_count": int(r[2]),
                "total_premium": float(r[3] or 0)
            })

        return jsonify({"status": "success", "data": rows})
    except Exception as e:
        logging.error(f"Growth error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/analytics/geo')
def analytics_geo():
    """Conversion by region.
    From dashboard_queries.md: select * from agg_location, aggregated by state."""
    try:
        conn = get_lakebase_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT state, SUM(trip_cnt) AS trip_count
                    FROM databricks_postgres.insurance_trip_wise.agg_location_sync
                    GROUP BY state
                    ORDER BY trip_count DESC
                """)
                rows = [{"state": r[0], "trip_count": int(r[1] or 0)} for r in cur.fetchall()]
        finally:
            conn.close()

        return jsonify({"status": "success", "data": rows})
    except Exception as e:
        logging.error(f"Geo error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/analytics/funnel')
def analytics_funnel():
    """Conversion stats from fact_offers."""
    try:
        sql = """
        SELECT
            COUNT(*) AS total_offers_shown,
            SUM(CASE WHEN was_clicked THEN 1 ELSE 0 END) AS total_offers_clicked,
            SUM(CASE WHEN did_sign_up THEN 1 ELSE 0 END) AS total_conversions
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.fact_offers
        """
        result = execute_sql_query(sql)
        if result["status"] != "success":
            return jsonify({"status": "error", "message": "Funnel query failed"}), 500

        fr = result["data"][0]
        shown = int(fr[0] or 0)
        clicked = int(fr[1] or 0)
        conversions = int(fr[2] or 0)
        click_rate = round(clicked * 100.0 / shown, 1) if shown else 0

        return jsonify({
            "status": "success",
            "data": {
                "total_offers_shown": shown,
                "total_offers_clicked": clicked,
                "total_conversions": conversions,
                "click_through_rate": click_rate
            }
        })
    except Exception as e:
        logging.error(f"Funnel error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/analytics/product_revenue')
def analytics_product_revenue():
    """Embedded revenue by product from agg_offer_performance."""
    try:
        sql = """
        SELECT product_name, CAST(SUM(premium_amount) AS DOUBLE) AS total_revenue
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies
        WHERE policy_type = 'embedded_trip' AND product_name IS NOT NULL
        GROUP BY product_name
        ORDER BY total_revenue DESC
        """
        result = execute_sql_query(sql)
        if result["status"] != "success":
            logging.error(f"Product revenue query failed: {result}")
            return jsonify({"status": "error", "message": "Product query failed"}), 500

        products = []
        for r in result["data"]:
            products.append({
                "product_name": r[0],
                "total_revenue": float(r[1] or 0)
            })

        return jsonify({"status": "success", "data": products})
    except Exception as e:
        logging.error(f"Product revenue error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/analytics/sankey')
def analytics_sankey():
    """Sankey: triggers -> trip phase -> products.
    From dashboard_queries.md: select * from agg_driver_conversions."""
    try:
        sql = """
        SELECT
            converting_trigger,
            trip_phase_at_conversion,
            converting_product,
            COUNT(*) AS cnt
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.agg_driver_conversions
        GROUP BY converting_trigger, trip_phase_at_conversion, converting_product
        ORDER BY converting_trigger, trip_phase_at_conversion, converting_product
        """
        result = execute_sql_query(sql)
        if result["status"] != "success":
            return jsonify({"status": "error", "message": "Query failed"}), 500

        # Collect unique values dynamically
        triggers_set = set()
        phases_set = set()
        products_set = set()
        for r in result["data"]:
            triggers_set.add(r[0])
            phases_set.add(r[1])
            products_set.add(r[2])

        triggers = sorted(triggers_set)
        phases = sorted(phases_set)
        products = sorted(products_set)

        nodes = triggers + phases + products
        node_map = {n: i for i, n in enumerate(nodes)}

        # Build links: trigger -> phase, phase -> product
        trigger_phase = {}
        phase_product = {}
        for r in result["data"]:
            trigger, phase, product = r[0], r[1], r[2]
            cnt = int(r[3])

            tp_key = (trigger, phase)
            trigger_phase[tp_key] = trigger_phase.get(tp_key, 0) + cnt

            pp_key = (phase, product)
            phase_product[pp_key] = phase_product.get(pp_key, 0) + cnt

        links = []
        for (trigger, phase), val in trigger_phase.items():
            if trigger in node_map and phase in node_map:
                links.append({"source": node_map[trigger], "target": node_map[phase], "value": val})

        for (phase, product), val in phase_product.items():
            if phase in node_map and product in node_map:
                links.append({"source": node_map[phase], "target": node_map[product], "value": val})

        return jsonify({
            "status": "success",
            "data": {
                "nodes": [{"name": n} for n in nodes],
                "links": links
            }
        })
    except Exception as e:
        logging.error(f"Sankey error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/analytics/tiers')
def analytics_tiers():
    """Driver distribution by tier with incidents.
    From dashboard_queries.md #driver distribution by tier."""
    try:
        sql = """
        SELECT
          min(driver_name) as driver_name,
          min(current_driving_score) as current_driving_score,
          min(total_miles) as total_miles,
          min(total_trips) as total_trips,
          min(risk_tier) as risk_tier,
          (sum(leaving_home_zone_count) + sum(rapid_accel_count) + sum(sharp_turn_count) + sum(rough_road_conditions_count)) as total_incidents
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers d
        JOIN serverless_stable_mv5gxq_catalog.insurance_trip_wise.trips t
          ON d.device_id = t.device_id
        GROUP BY d.device_id
        """
        result = execute_sql_query(sql)
        if result["status"] != "success":
            return jsonify({"status": "error", "message": "Query failed"}), 500

        # Aggregate by risk_tier for the chart
        tier_data = {}
        for r in result["data"]:
            tier = r[4]
            if not tier:
                continue
            if tier not in tier_data:
                tier_data[tier] = {"driver_count": 0, "total_score": 0.0, "total_incidents": 0}
            tier_data[tier]["driver_count"] += 1
            tier_data[tier]["total_score"] += float(r[1] or 0)
            tier_data[tier]["total_incidents"] += int(r[5] or 0)

        rows = []
        for tier in sorted(tier_data.keys()):
            d = tier_data[tier]
            rows.append({
                "risk_tier": tier,
                "driver_count": d["driver_count"],
                "avg_score": round(d["total_score"] / d["driver_count"], 1),
                "total_incidents": d["total_incidents"]
            })

        return jsonify({"status": "success", "data": rows})
    except Exception as e:
        logging.error(f"Tiers error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/analytics/score_heatmap')
def analytics_score_heatmap():
    """Driver score distribution scatter: total_miles vs driving_score, colored by risk_tier.
    From dashboard_queries.md #driver distribution by tier."""
    try:
        sql = """
        SELECT
          min(driver_name) as driver_name,
          min(current_driving_score) as current_driving_score,
          min(total_miles) as total_miles,
          min(risk_tier) as risk_tier
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers d
        JOIN serverless_stable_mv5gxq_catalog.insurance_trip_wise.trips t
          ON d.device_id = t.device_id
        WHERE d.risk_tier IS NOT NULL
        GROUP BY d.device_id
        """
        result = execute_sql_query(sql)
        if result["status"] != "success":
            return jsonify({"status": "error", "message": "Query failed"}), 500

        rows = []
        for r in result["data"]:
            rows.append({
                "driver_name": r[0],
                "current_driving_score": float(r[1] or 0),
                "total_miles": float(r[2] or 0),
                "risk_tier": r[3]
            })

        return jsonify({"status": "success", "data": rows})
    except Exception as e:
        logging.error(f"Score heatmap error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# GENIE API ENDPOINTS
# ============================================================================

@app.route('/api/genie/start_conversation', methods=['POST'])
def genie_start_conversation():
    data = request.json
    question = data.get('question', '')
    if not question:
        return jsonify({"error": "No question provided"}), 400

    try:
        token = get_databricks_oauth_token()
        space_id = get_genie_space_id()
        logging.info(f"Genie question: {question}, space_id: {space_id}")

        response = new_genie_conversation(
            space_id=space_id, content=question,
            token=token, databricks_host=databricks_host
        )

        return jsonify({
            "content": response.get('query_result') or response.get('content') or str(response),
            "conversation_id": response.get('conversation_id'),
            "message_id": response.get('message_id')
        })
    except Exception as e:
        logging.error(f"Genie error: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/genie/continue_conversation', methods=['POST'])
def genie_continue():
    data = request.get_json()
    question = data.get('question', '')
    conversation_id = data.get('conversation_id', '')
    if not question or not conversation_id:
        return jsonify({"error": "Missing parameters"}), 400

    try:
        token = get_databricks_oauth_token()
        space_id = get_genie_space_id()

        response = continue_genie_conversation(
            space_id=space_id, content=question,
            conversation_id=conversation_id,
            token=token, databricks_host=databricks_host
        )

        return jsonify({
            "content": response.get('query_result') or response.get('content') or str(response),
            "conversation_id": response.get('conversation_id'),
            "message_id": response.get('message_id')
        })
    except Exception as e:
        logging.error(f"Genie continue error: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# DRIVER DATA API
# ============================================================================

@app.route('/api/drivers', methods=['GET'])
def get_drivers():
    try:
        # Pull the driver list from Lakebase. device_id + driver_name come from the
        # registration table (tripwise_demo.devices); the behavioral fields
        # (current_driving_score / risk_tier / trips / miles) come from
        # insurance_trip_wise.dim_driver_sync, joined on device_id.
        conn = get_lakebase_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT dev.device_id, dev.driver_name,
                           s.vehicle_info, s.current_driving_score, s.total_trips,
                           s.total_miles, s.risk_tier, s.status
                    FROM tripwise_demo.devices dev
                    JOIN insurance_trip_wise.dim_driver_sync s
                        ON dev.device_id = s.device_id
                    WHERE s.status = 'active'
                    ORDER BY s.current_driving_score DESC
                """)
                columns = [desc[0] for desc in cur.description]
                drivers = [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()

        return jsonify({"status": "success", "data": drivers})

    except Exception as e:
        logging.error(f"Error fetching drivers: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/driver/<device_id>', methods=['GET'])
def get_driver_details(device_id):
    try:
        # driver_name comes from the Lakebase devices registration table (via its
        # online UC catalog av_lakebase_tripwise.tripwise_demo.devices) so the name
        # matches the Select Driver list exactly; behavioral + policy fields stay on Delta.
        sql = f"""
        SELECT
            d.device_id, dev.driver_name, d.vehicle_info, d.policy_number,
            d.registration_date, d.current_driving_score, d.total_trips,
            d.total_miles, d.risk_tier, d.status,
            p.policy_id, p.premium_amount, p.premium_period, p.discount_pct,
            p.coverage_level, p.effective_date, p.is_active as policy_is_active
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.dim_drivers d
        LEFT JOIN serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies p
            ON d.device_id = p.device_id AND p.is_active = true
        LEFT JOIN av_lakebase_tripwise.tripwise_demo.devices dev
            ON d.device_id = dev.device_id
        WHERE d.device_id = '{device_id}'
        AND p.policy_type != 'embedded_trip'
        LIMIT 1
        """
        result = execute_sql_query(sql)

        if result["status"] == "success" and result.get("data"):
            raw_data = result["data"]
            if raw_data and len(raw_data) > 0:
                row = raw_data[0]
                columns = [col.name for col in result["columns"]] if result.get("columns") else [
                    "device_id", "driver_name", "vehicle_info", "policy_number",
                    "registration_date", "current_driving_score", "total_trips",
                    "total_miles", "risk_tier", "status", "policy_id", "premium_amount",
                    "premium_period", "discount_pct", "coverage_level", "effective_date",
                    "policy_is_active"]
                driver = {columns[i]: v for i, v in enumerate(row) if i < len(columns)}
                return jsonify({"status": "success", "data": driver})

        return jsonify({"status": "error", "message": "Driver not found"}), 404

    except Exception as e:
        logging.error(f"Error fetching driver details: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# AI FORECAST API
# ============================================================================

@app.route('/api/driver/<device_id>/ai_forecast', methods=['POST'])
def ai_forecast_premium(device_id):
    """Use driver trip behavior data + AI to forecast recommended premium and discount."""
    try:
        data = request.json
        current_premium = float(data.get('current_premium', 0))
        current_discount = float(data.get('current_discount', 0))

        # 1. Get aggregated trip behavior data for this driver
        behavior_sql = f"""
        SELECT
            COUNT(*) as total_trips,
            ROUND(AVG(trip_score), 1) as avg_trip_score,
            ROUND(AVG(distance_miles), 1) as avg_distance,
            ROUND(AVG(avg_speed_mph), 1) as avg_speed,
            ROUND(MAX(max_speed_mph), 1) as max_speed_ever,
            SUM(leaving_home_zone_count) as total_leaving_home,
            SUM(rapid_accel_count) as total_rapid_accel,
            SUM(sharp_turn_count) as total_sharp_turns,
            SUM(rough_road_conditions_count) as total_rough_road,
            ROUND(AVG(night_driving_pct), 1) as avg_night_driving_pct,
            ROUND(AVG(high_risk_area_pct), 1) as avg_high_risk_pct,
            ROUND(AVG(inclement_weather_pct), 1) as avg_weather_pct,
            SUM(CASE WHEN did_convert = true THEN 1 ELSE 0 END) as total_conversions,
            SUM(offers_shown) as total_offers_shown,
            SUM(offers_clicked) as total_offers_clicked
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.trips
        WHERE device_id = '{device_id}'
        """
        behavior_result = execute_sql_query(behavior_sql)

        if behavior_result["status"] != "success" or not behavior_result.get("data"):
            return jsonify({"status": "error", "message": "No trip data found for driver"}), 404

        row = behavior_result["data"][0]
        cols = [c.name for c in behavior_result["columns"]]
        behavior = {cols[i]: row[i] for i in range(len(cols))}

        # 2. Get monthly trend data for scoring trajectory
        trend_sql = f"""
        SELECT
            DATE_TRUNC('month', trip_date) as month,
            ROUND(AVG(trip_score), 1) as avg_score,
            COUNT(*) as trip_count,
            SUM(leaving_home_zone_count + rapid_accel_count + sharp_turn_count + rough_road_conditions_count) as total_incidents
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.trips
        WHERE device_id = '{device_id}'
        GROUP BY DATE_TRUNC('month', trip_date)
        ORDER BY month
        """
        trend_result = execute_sql_query(trend_sql)
        trend_data = []
        if trend_result["status"] == "success" and trend_result.get("data"):
            for r in trend_result["data"]:
                trend_data.append({"month": str(r[0]), "avg_score": r[1], "trips": r[2], "incidents": r[3]})

        # 3. Use LLM to analyze behavior and recommend premium/discount
        total_incidents = (int(behavior.get('total_rapid_accel') or 0) +
                          int(behavior.get('total_sharp_turns') or 0) +
                          int(behavior.get('total_rough_road') or 0) +
                          int(behavior.get('total_leaving_home') or 0))

        prompt = f"""You are an insurance actuary AI analyzing driver behavior data to recommend premium and discount adjustments.

Current Policy:
- Current Premium: ${current_premium:.2f}
- Current Discount: {current_discount}%

Driver Behavior Summary:
- Total Trips: {behavior.get('total_trips', 0)}
- Average Trip Score: {behavior.get('avg_trip_score', 0)}/100
- Average Distance per Trip: {behavior.get('avg_distance', 0)} miles
- Average Speed: {behavior.get('avg_speed', 0)} mph
- Max Speed Ever Recorded: {behavior.get('max_speed_ever', 0)} mph
- Total Incidents: {total_incidents} (rapid accel: {behavior.get('total_rapid_accel', 0)}, sharp turns: {behavior.get('total_sharp_turns', 0)}, rough road: {behavior.get('total_rough_road', 0)}, leaving home zone: {behavior.get('total_leaving_home', 0)})
- Night Driving: {behavior.get('avg_night_driving_pct', 0)}%
- High Risk Area Driving: {behavior.get('avg_high_risk_pct', 0)}%
- Inclement Weather Driving: {behavior.get('avg_weather_pct', 0)}%
- Offer Engagement: {behavior.get('total_offers_clicked', 0)} clicked of {behavior.get('total_offers_shown', 0)} shown
- Conversions: {behavior.get('total_conversions', 0)}

Monthly Score Trend (recent):
{chr(10).join([f"  {t['month']}: score={t['avg_score']}, trips={t['trips']}, incidents={t['incidents']}" for t in trend_data[-6:]])}

Based on this data, provide:
1. A recommended new premium amount (adjust up or down from ${current_premium:.2f} based on risk — max premium $2500)
2. A recommended discount percentage (0-50%, reward safe drivers)
3. A brief explanation (2-3 sentences) of why

IMPORTANT: Respond in EXACTLY this JSON format, nothing else:
{{"new_premium": 1234.56, "new_discount": 15, "explanation": "Your explanation here.", "risk_assessment": "low|medium|high"}}"""

        endpoint = os.environ.get('LLM_ENDPOINT', 'databricks-meta-llama-3-3-70b-instruct')
        response = w.serving_endpoints.query(
            name=endpoint,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM,
                           content="You are a precise insurance actuary AI. Always respond with valid JSON only."),
                ChatMessage(role=ChatMessageRole.USER, content=prompt)
            ],
            max_tokens=300
        )

        llm_text = response.choices[0].message.content.strip()

        # Parse the JSON from LLM response
        import json as json_lib
        import re
        try:
            forecast = json_lib.loads(llm_text)
        except json_lib.JSONDecodeError:
            json_match = re.search(r'\{[^}]+\}', llm_text, re.DOTALL)
            if json_match:
                forecast = json_lib.loads(json_match.group())
            else:
                return jsonify({"status": "error", "message": "Failed to parse AI response"}), 500

        return jsonify({
            "status": "success",
            "forecast": {
                "new_premium": round(float(forecast.get("new_premium", current_premium)), 2),
                "new_discount": min(50, max(0, int(forecast.get("new_discount", current_discount)))),
                "explanation": forecast.get("explanation", ""),
                "risk_assessment": forecast.get("risk_assessment", "medium")
            },
            "behavior_summary": {
                "total_trips": behavior.get("total_trips"),
                "avg_trip_score": behavior.get("avg_trip_score"),
                "total_incidents": total_incidents,
                "avg_night_driving_pct": behavior.get("avg_night_driving_pct"),
                "avg_high_risk_pct": behavior.get("avg_high_risk_pct")
            },
            "trend": trend_data[-6:]
        })

    except Exception as e:
        logging.error(f"AI forecast error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# UPDATE POLICY API
# ============================================================================

@app.route('/api/driver/<device_id>/update_policy', methods=['POST'])
def update_driver_policy(device_id):
    try:
        data = request.json
        new_premium = data.get('new_premium')
        new_discount = data.get('new_discount')
        policy_id = data.get('policy_id')

        if new_premium is None and new_discount is None:
            return jsonify({"status": "error", "message": "No update values provided"}), 400

        sql = query_config.get_update_policy_sql(device_id, policy_id, new_premium, new_discount)
        result = execute_sql_query(sql)

        if result["status"] == "success":
            return jsonify({
                "status": "success",
                "message": f"Policy updated. Premium: ${new_premium}, Discount: {new_discount}%"
            })
        else:
            return jsonify({"status": "error", "message": result.get("error", "Update failed")}), 500

    except Exception as e:
        logging.error(f"Error updating policy: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# PREMIUM FORECAST API  (calls premium_forecast_tripwise serving endpoint)
# ============================================================================

@app.route('/api/driver/<device_id>/forecast', methods=['GET'])
def forecast_driver_policy(device_id):
    """
    Fetches the driver's feature row from ml_premium_features and sends it
    to the premium_forecast_tripwise model serving endpoint.
    The model predicts discount_pct; adjusted_premium is derived as:
      undiscounted_base = current_premium / (1 - current_discount/100)
      adjusted_premium  = undiscounted_base * (1 - predicted_discount/100)
    premium_amount and discount_pct are fetched live from the policies table (single source of truth).
    """
    try:
        # Fetch ML features from the feature table
        features_sql = f"""
        SELECT
            current_driving_score, total_trips, total_miles,
            avg_trip_score, avg_trip_distance_miles, avg_speed_mph,
            avg_rapid_accel_count, avg_sharp_turn_count,
            avg_leaving_home_zone_count, avg_rough_road_conditions_count,
            avg_night_driving_pct, avg_high_risk_area_pct,
            avg_inclement_weather_pct, embedded_conversion_rate,
            risk_tier, state, coverage_level, policy_type
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.ml_premium_features
        WHERE device_id = '{device_id}'
        LIMIT 1
        """
        result = execute_sql_query(features_sql)

        if result["status"] != "success" or not result.get("data"):
            return jsonify({"status": "error", "message": "Driver features not found"}), 404

        row = result["data"][0]
        columns = [col.name for col in result["columns"]]
        features = {columns[i]: v for i, v in enumerate(row)}

        # Fetch live premium and discount from the policies table
        policy_sql = f"""
        SELECT premium_amount, discount_pct
        FROM serverless_stable_mv5gxq_catalog.insurance_trip_wise.policies
        WHERE device_id = '{device_id}' AND is_active = true AND policy_type != 'embedded_trip'
        ORDER BY effective_date DESC
        LIMIT 1
        """
        policy_result = execute_sql_query(policy_sql)

        if policy_result["status"] != "success" or not policy_result.get("data"):
            return jsonify({"status": "error", "message": "Active policy not found"}), 404

        policy_row = policy_result["data"][0]
        current_premium  = float(policy_row[0] or 0)
        current_discount = float(policy_row[1] or 0)

        # Cast numeric fields — OrdinalEncoder expects str for categoricals
        numeric_fields = [
            "current_driving_score", "total_trips", "total_miles",
            "avg_trip_score", "avg_trip_distance_miles", "avg_speed_mph",
            "avg_rapid_accel_count", "avg_sharp_turn_count",
            "avg_leaving_home_zone_count", "avg_rough_road_conditions_count",
            "avg_night_driving_pct", "avg_high_risk_area_pct",
            "avg_inclement_weather_pct", "embedded_conversion_rate",
        ]
        for field in numeric_fields:
            if features.get(field) is not None:
                features[field] = float(features[field])

        response = w.serving_endpoints.query(
            name=PREMIUM_FORECAST_ENDPOINT,
            dataframe_records=[features],
        )

        preds = response.predictions
        # Model outputs a single value: discount_pct
        predicted_discount = round(max(0.0, min(50.0, float(preds[0]))), 1)

        # The model predicts discount_pct; adjusted_premium is derived as:
        undiscounted_base = current_premium / (1 - current_discount / 100) if current_discount < 100 else current_premium
        predicted_premium = round(undiscounted_base * (1 - predicted_discount / 100), 2)

        return jsonify({
            "status": "success",
            "predicted_premium": predicted_premium,
            "predicted_discount_pct": predicted_discount,
        })

    except Exception as e:
        logging.error(f"Forecast error for {device_id}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# EMAIL GENERATION API
# ============================================================================

@app.route('/api/generate_email', methods=['POST'])
def generate_email():
    try:
        data = request.json
        driver_data = data.get('driver_data', {})
        email_type = data.get('email_type', 'retention_premium')

        if not driver_data:
            return jsonify({"error": "No driver data provided"}), 400

        email_content = generate_driver_email(driver_data, email_type)
        return jsonify({"status": "success", "email_content": email_content})

    except Exception as e:
        logging.error(f"Error generating email: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


def generate_driver_email(driver_data, email_type):
    driver_name = driver_data.get('driver_name', 'Valued Customer')
    driving_score = float(driver_data.get('current_driving_score') or 0)
    total_trips = int(driver_data.get('total_trips') or 0)
    total_miles = float(driver_data.get('total_miles') or 0)
    risk_tier = driver_data.get('risk_tier', 'standard')
    vehicle_info = driver_data.get('vehicle_info', '')
    current_premium = float(driver_data.get('premium_amount') or 0)
    current_discount = float(driver_data.get('discount_pct') or 0)
    coverage_level = driver_data.get('coverage_level', 'standard')
    new_premium = float(driver_data.get('new_premium') or current_premium)
    new_discount = float(driver_data.get('new_discount') or current_discount)

    if email_type == "retention_premium":
        prompt = f"""You are a friendly insurance agent at Tripwise writing a personalized email to retain a valued customer by offering them a premium reduction.

Driver Details:
- Name: {driver_name}
- Vehicle: {vehicle_info}
- Driving Score: {driving_score}/100
- Risk Tier: {risk_tier}
- Total Trips Completed: {total_trips}
- Total Miles Driven: {total_miles:,.1f}

Policy Details:
- Coverage Level: {coverage_level}
- Previous Premium: ${current_premium:.2f}/month
- New Premium: ${new_premium:.2f}/month
- Previous Discount: {current_discount}%
- New Discount: {new_discount}%

Write a warm, professional email congratulating them on their excellent driving record and informing them of their new lower premium.
Keep it concise (under 200 words). Start with a subject line."""

    elif email_type == "retention_loyalty":
        prompt = f"""You are a friendly insurance agent at Tripwise writing a personalized email to reward a loyal customer.

Driver Details:
- Name: {driver_name}
- Driving Score: {driving_score}/100
- Total Trips: {total_trips}
- Total Miles: {total_miles:,.1f}

Policy Details:
- Coverage Level: {coverage_level}
- Previous Premium: ${current_premium:.2f}/month
- New Premium: ${new_premium:.2f}/month
- Previous Discount: {current_discount}%
- New Discount: {new_discount}%

Write a warm email thanking them for their loyalty and informing them of a special loyalty discount.
Keep it concise (under 200 words). Start with a subject line."""

    else:
        prompt = f"""You are a friendly insurance agent at Tripwise writing a compelling email to acquire a new customer.

Prospect Details:
- Name: {driver_name}
- Driving Score: {driving_score}/100
- Risk Assessment: {risk_tier}

Offer Details:
- Offered Premium: ${new_premium:.2f}/month
- Special Discount: {new_discount}%

Write a compelling email introducing Tripwise embedded insurance. Highlight:
- Real-time discounts based on driving behavior
- Pay-per-trip coverage options
- Easy mobile app enrollment

Keep it concise (under 200 words). Start with a subject line."""

    try:
        endpoint = os.environ.get('LLM_ENDPOINT', 'databricks-meta-llama-3-3-70b-instruct')
        response = w.serving_endpoints.query(
            name=endpoint,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM,
                           content="You are a professional, friendly insurance agent assistant."),
                ChatMessage(role=ChatMessageRole.USER, content=prompt)
            ],
            max_tokens=500
        )
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"LLM error: {e}")
        return f"Error generating email: {str(e)}"


# ============================================================================
# OFFER ACTION API
# ============================================================================

def lat_lon_to_state(lat, lon):
    """Map lat/lon to state by finding the closest known region center."""
    if lat is None or lon is None:
        return None
    lat = float(lat)
    lon = float(lon)

    # Region centers (avg of landmark coords) mapped to full state names
    REGION_CENTERS = {
        "New York":              (40.7559, -73.9466),
        "Massachusetts":         (42.3590, -71.0825),
        "Pennsylvania":          (39.9379, -75.1648),
        "Washington, D.C.":      (38.9041, -77.0312),
        "Florida":               (25.7722, -80.1882),
        "Georgia":               (33.7827, -84.3635),
        "Illinois":              (41.8849, -87.6466),
        "Texas - Dallas":        (32.8237, -96.8214),
        "Texas - Houston":       (29.7586, -95.3769),
        "Arizona":               (33.4525, -111.9530),
        "Colorado":              (39.7353, -104.9887),
        "Washington":            (47.6237, -122.3431),
        "California - LA":       (34.0268, -118.3679),
        "California - SF":       (37.7820, -122.4148),
    }

    best_state = None
    best_dist = float('inf')
    for state, (clat, clon) in REGION_CENTERS.items():
        dist = (lat - clat) ** 2 + (lon - clon) ** 2
        if dist < best_dist:
            best_dist = dist
            best_state = state

    # Normalize Texas/California sub-regions to state name
    if best_state and best_state.startswith("Texas"):
        return "Texas"
    if best_state and best_state.startswith("California"):
        return "California"
    return best_state


@app.route('/api/lakebase_devices', methods=['GET'])
def get_lakebase_devices():
    try:
        conn = get_lakebase_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT device_id, driver_name, registered_at,
                           registration_latitude, registration_longitude
                    FROM devices
                    ORDER BY registered_at DESC
                """)
                columns = [desc[0] for desc in cur.description]
                devices = [dict(zip(columns, row)) for row in cur.fetchall()]
        finally:
            conn.close()

        return jsonify({"status": "success", "data": devices})

    except Exception as e:
        logging.error(f"Error fetching Lakebase devices: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/offer_recommendation/<device_id>', methods=['GET'])
def get_offer_recommendation(device_id):
    try:
        conn = get_lakebase_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT d.device_id, d.driver_name, d.registered_at, "
                    "d.registration_latitude, d.registration_longitude, "
                    "s.current_driving_score "
                    "FROM tripwise_demo.devices d "
                    "LEFT JOIN insurance_trip_wise.dim_driver_sync s "
                    "ON d.device_id = s.device_id "
                    "WHERE d.device_id = %s LIMIT 1",
                    (device_id,)
                )
                row = cur.fetchone()
                if not row:
                    return jsonify({"status": "error", "message": "Device not found"}), 404
                columns = [desc[0] for desc in cur.description]
                device = dict(zip(columns, row))
        finally:
            conn.close()

        lat = device.get("registration_latitude")
        lon = device.get("registration_longitude")
        state = lat_lon_to_state(lat, lon)

        if not state:
            return jsonify({
                "status": "success", "device": device,
                "state": None, "recommendation": None,
                "message": "Could not determine state from coordinates"
            })

        # Query agg_location_sync from Lakebase directly
        conn2 = get_lakebase_connection()
        try:
            with conn2.cursor() as cur2:
                cur2.execute(
                    "SELECT converting_trigger "
                    "FROM databricks_postgres.insurance_trip_wise.agg_location_sync "
                    "WHERE state = %s "
                    "ORDER BY trip_cnt DESC LIMIT 1",
                    (state,)
                )
                agg_row = cur2.fetchone()
        finally:
            conn2.close()

        trigger = agg_row[0] if agg_row else None

        product = TRIGGER_TO_PRODUCT.get(trigger) if trigger else None

        return jsonify({
            "status": "success", "device": device,
            "state": state, "converting_trigger": trigger,
            "recommendation": product
        })

    except Exception as e:
        logging.error(f"Error getting offer recommendation: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/generate_offer_email', methods=['POST'])
def generate_offer_email():
    try:
        data = request.json
        driver_name = data.get('driver_name', 'Valued Customer')
        state = data.get('state', '')
        trigger = data.get('converting_trigger', '')
        product = data.get('product', {})
        device_id = data.get('device_id', '')

        if not product:
            return jsonify({"error": "No product recommendation available"}), 400

        prompt = f"""You are a friendly insurance agent at Tripwise writing a personalized email to offer an embedded per-trip insurance product to a newly registered driver.

Driver Details:
- Name: {driver_name}
- Device ID: {device_id}
- Registration State: {state}

Offer Details:
- Product: {product.get('name', '')}
- Price: {product.get('price', '')}
- Why this product: Based on driving data analysis in {state}, the most common risk trigger is "{trigger}". This product provides coverage specifically for this risk.

Write a compelling, warm email introducing this specific embedded insurance product. Explain:
- How the product works (per-trip, automatic activation)
- Why it's recommended for drivers in {state} based on regional driving patterns
- The affordable per-trip pricing
- Easy enrollment through the Tripwise app

Keep it concise (under 250 words). Start with a subject line."""

        endpoint = os.environ.get('LLM_ENDPOINT', 'databricks-meta-llama-3-3-70b-instruct')
        response = w.serving_endpoints.query(
            name=endpoint,
            messages=[
                ChatMessage(role=ChatMessageRole.SYSTEM,
                           content="You are a professional, friendly insurance agent assistant."),
                ChatMessage(role=ChatMessageRole.USER, content=prompt)
            ],
            max_tokens=600
        )
        return jsonify({"status": "success", "email_content": response.choices[0].message.content})

    except Exception as e:
        logging.error(f"Error generating offer email: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500


# ============================================================================
# DEVICE REGISTRATION APIs
# ============================================================================

@app.route('/api/device_count', methods=['GET'])
def get_device_count():
    try:
        conn = get_lakebase_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM devices")
                count = cur.fetchone()[0]
        finally:
            conn.close()
        return jsonify({"status": "success", "count": count})
    except Exception as e:
        logging.error(f"Error fetching device count: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/registration_timeline', methods=['GET'])
def get_registration_timeline():
    try:
        conn = get_lakebase_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        DATE_TRUNC('second', registered_at) as reg_second,
                        COUNT(*) as devices_per_second
                    FROM devices
                    GROUP BY DATE_TRUNC('second', registered_at)
                    ORDER BY reg_second
                """)
                timeline = []
                for row in cur.fetchall():
                    timeline.append({
                        "reg_second": row[0].isoformat() if hasattr(row[0], 'isoformat') else str(row[0]),
                        "devices_per_second": int(row[1])
                    })
        finally:
            conn.close()

        return jsonify({"status": "success", "data": timeline})

    except Exception as e:
        logging.error(f"Error fetching registration timeline: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/truncate_devices', methods=['POST'])
def truncate_devices():
    try:
        conn = get_lakebase_connection()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE devices")
        finally:
            conn.close()

        return jsonify({"status": "success", "message": "Devices table truncated successfully"})

    except Exception as e:
        logging.error(f"Error truncating devices: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/trigger_registration_job', methods=['POST'])
def trigger_registration_job():
    try:
        job_id = 213721155994897
        run = w.jobs.run_now(job_id=job_id)
        return jsonify({
            "status": "success", "message": "Job triggered successfully",
            "run_id": run.run_id
        })
    except Exception as e:
        logging.error(f"Error triggering job: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ============================================================================
# HELPERS
# ============================================================================

def get_user_workspace_client():
    """Use the SP-based WorkspaceClient for SQL queries."""
    return w


def execute_sql_query(sql_query):
    warehouse_id = query_config.get_warehouse_id()
    try:
        client = get_user_workspace_client()
        statement = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql_query
        )
        if statement.status.state == StatementState.SUCCEEDED:
            return {
                "status": "success",
                "data": statement.result.data_array,
                "columns": statement.manifest.schema.columns,
                "row_count": statement.result.row_count
            }
        elif statement.status.state == StatementState.FAILED:
            logging.error(f"SQL failed: {statement.status.error.message if statement.status.error else 'Unknown'}")
            return {"status": "error", "error": statement.status.error.message if statement.status.error else "Unknown"}
        return {"status": "pending", "statement_id": statement.statement_id}
    except Exception as e:
        logging.error(f"execute_sql_query error: {e}")
        return {"status": "error", "error": str(e)}


def extract_name(email):
    try:
        local = email.split('@')[0]
        return ' '.join(p.capitalize() for p in local.split('.'))
    except:
        return email or 'Agent'


def extract_first_name(email):
    try:
        return email.split('@')[0].split('.')[0].capitalize()
    except:
        return email or 'Agent'


if __name__ == '__main__':
    app.run(debug=True)
