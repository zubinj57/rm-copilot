import pandas as pd
from sqlalchemy import create_engine

import traceback
from sqlalchemy import text
from datetime import datetime, date
from decimal import Decimal 

def fetch_data(conn, query, params=None):
    """Fetch data using a cursor, format results, and return them as a list of dictionaries."""
    formatted_results = []
    result = conn.execute(text(query), params or {})
    print(result)

    columns = result.keys()  # Get column names
    for row in result.fetchall():
        row_dict = dict(zip(columns, row))
        for key, value in row_dict.items():
            if isinstance(value, (datetime, date)):  
                row_dict[key] = value.strftime("%Y-%m-%d")
            elif isinstance(value, Decimal):  
                row_dict[key] = float(value)
        formatted_results.append(row_dict)
    
    return formatted_results
   
# -----------------------------------------------------------------------------
# Data Fetching
# -----------------------------------------------------------------------------
def fetch_daily_summaries(propertyCode, AsOfDate) -> pd.DataFrame:
    """Fetch latest daily summaries from Postgres."""
    q =f"""
   SELECT
    "AsOfDate",
    "Dates",
    "Inventory",
    "RoomSold",
    "TotalRevenue",
    "ADR",
    "AvailableOccupancy",
    "RevPAR",
    "Occperc",
    "OutOfOrder",
    "RoomsOfAvailable",
    "DayOfWeek",
    "WeekType",
    "GroupADR",
    "GroupBlock",
    "GroupOTB",
    "GroupRevenue",
    "TransientRoomSold",
    "TransientRevenue",
    "TransientADR",
    "LYTotalInventory",
    "LYTotalRoomSold",
    "LYTotalRevenue",
    "LYTotalADR",
    "LYTotalOccupancy",
    "LYTotalRevPar",
    "LYTotalOccPerc",
    "LYPaceInventory",
    "LYPaceRoomSold",
    "LYPaceRevenue",
    "LYPaceADR",
    "LYPaceOccupancy",
    "LYPaceRevPar",
    "LYPaceOccPerc"
FROM dailydata_transaction
WHERE
"propertyCode" = '{propertyCode}'
and "AsOfDate" = '{AsOfDate}'
and "Dates" BETWEEN (DATE '{AsOfDate}' - INTERVAL '1 month')
                    AND (DATE '{AsOfDate}' + INTERVAL '3 month')
ORDER BY "AsOfDate" DESC;"""
 
 
 
    engine = create_engine(f"postgresql+psycopg2://postgres:9MGPMPiDn2RdegC2QMhc@backup-db-ema-postgres.cryru6bacdry.us-east-1.rds.amazonaws.com:5432/{propertyCode}")
    with engine.connect() as conn:
        return pd.read_sql(q, conn)

def fetch_reservation(propertyCode, AsOfDate) -> pd.DataFrame:
    """Fetch latest daily summaries from Postgres."""
    q =f"""
   SELECT
    "AsOfDate",
    "Dates",
    "Inventory",
    "RoomSold",
    "TotalRevenue",
    "ADR",
    "AvailableOccupancy",
    "RevPAR",
    "Occperc",
    "OutOfOrder",
    "RoomsOfAvailable",
    "DayOfWeek",
    "WeekType",
    "GroupADR",
    "GroupBlock",
    "GroupOTB",
    "GroupRevenue",
    "TransientRoomSold",
    "TransientRevenue",
    "TransientADR",
    "LYTotalInventory",
    "LYTotalRoomSold",
    "LYTotalRevenue",
    "LYTotalADR",
    "LYTotalOccupancy",
    "LYTotalRevPar",
    "LYTotalOccPerc",
    "LYPaceInventory",
    "LYPaceRoomSold",
    "LYPaceRevenue",
    "LYPaceADR",
    "LYPaceOccupancy",
    "LYPaceRevPar",
    "LYPaceOccPerc"
FROM dailydata_transaction
WHERE
"propertyCode" = '{propertyCode}'
and "AsOfDate" = '{AsOfDate}'
and "Dates" BETWEEN (CURRENT_DATE - INTERVAL '1 month')
                    AND (CURRENT_DATE + INTERVAL '3 month')
ORDER BY "AsOfDate" DESC;"""
 
 
 
    engine = create_engine("postgresql+psycopg2://postgres:9MGPMPiDn2RdegC2QMhc@backup-db-ema-postgres.cryru6bacdry.us-east-1.rds.amazonaws.com:5432/AC32AW")
    with engine.connect() as conn:
        return pd.read_sql(q, conn)

def get_PerformanceMonitor(PROPERTY_ID, PROPERTY_CODE, AS_OF_DATE, CLIENT_ID, conn):
    try:
        start_date = end_date = AS_OF_DATE
        error_list = []
        response_json = None

        daily_performance_dashboard_query = f"""
          WITH latest_date AS (
                          SELECT MAX("AsOfDate") AS asofdate
                          FROM dailydata_transaction
                      ),
                      
                      target_date AS (
                          SELECT "Dates" AS target_date
                          FROM dailydata_transaction
                          WHERE "AsOfDate" = (SELECT asofdate FROM latest_date)
                            AND "Dates" = "AsOfDate"
                          LIMIT 1
                      ),
                      
                      past_8_weeks AS (
                          SELECT generate_series(
                              (SELECT target_date FROM target_date) - INTERVAL '56 days',
                              (SELECT target_date FROM target_date) - INTERVAL '7 days',
                              INTERVAL '7 days'
                          )::date AS past_weeks
                      ),
                      
                      adr_history AS (
                          SELECT dt."Dates", dt."ADR"
                          FROM dailydata_transaction dt
                          JOIN past_8_weeks pw ON dt."Dates" = pw.past_weeks
                          WHERE dt."AsOfDate" = (SELECT asofdate FROM latest_date)
                      ),
                      
                      today_adr AS (
                          SELECT "ADR"
                          FROM dailydata_transaction
                          WHERE "AsOfDate" = (SELECT asofdate FROM latest_date)
                            AND "Dates" = "AsOfDate"
                          LIMIT 1
                      ),
                      
                      adr_rolling_stats AS (
                          SELECT
                              GREATEST(MAX(adr_hist."ADR"), MAX(today."ADR")) AS "Highest_ADR_Past_8_Weeks",
                              LEAST(MIN(adr_hist."ADR"), MIN(today."ADR")) AS "Lowest_ADR_Past_8_Weeks"
                          FROM adr_history adr_hist
                          CROSS JOIN today_adr today
                      ),
                      
                      rate_extremes AS (
                          SELECT
                              MAX("Rate") AS max_rate,
                              MIN("Rate") AS min_rate
                          FROM copy_mst_reservation
                          WHERE "AsOfDate" = (SELECT asofdate FROM latest_date)
                            AND "StayDate" = "AsOfDate"
                      ),
                      
                      max_bookings AS (
                          SELECT
                              "Rate" AS max_rate,
                              "BookingDate" AS max_booking_date,
                              "BookingTime" AS max_booking_time
                          FROM copy_mst_reservation
                          WHERE "AsOfDate" = (SELECT asofdate FROM latest_date)
                            AND "StayDate" = "AsOfDate"
                            AND "Rate" = (SELECT max_rate FROM rate_extremes)
                          LIMIT 1
                      ),
                      
                      min_bookings AS (
                          SELECT
                              "Rate" AS min_rate,
                              "BookingDate" AS min_booking_date,
                              "BookingTime" AS min_booking_time
                          FROM copy_mst_reservation
                          WHERE "AsOfDate" = (SELECT asofdate FROM latest_date)
                            AND "StayDate" = "AsOfDate"
                            AND "Rate" = (SELECT min_rate FROM rate_extremes)
                          LIMIT 1
                      ),
                      
                      index_avg AS (
                          SELECT
                              ROUND(AVG("OccIndex"), 2) AS "Average_MPI",
                              ROUND(AVG("AdrIndex"), 2) AS "Average_ARI",
                              ROUND(AVG("RevparIndex"), 2) AS "Average_RGI"
                          FROM snp_weekly_star sws
                          WHERE "AsOfDate" = (SELECT MAX("AsOfDate") FROM snp_weekly_star)
                            AND "StarReportDate" IS NOT NULL
                      ),
                      
                      bar_rate_avg AS (
                          SELECT
                              ROUND(AVG("Rate"), 2) AS "BAR_Rate"
                          FROM copy_mst_reservation
                          WHERE "AsOfDate" = (SELECT asofdate FROM latest_date)
                            AND "StayDate" = "AsOfDate"
                            AND "BarBased" = 'Y'
                      )
                      
                      SELECT
                          dt."Dates",
                          dt."ADR",
                          dt."ADR" - dt."YestDayADR" AS "ADR Difference",
                          CASE
                              WHEN dt."YestDayADR" IS NULL OR dt."YestDayADR" = 0 THEN 100
                              ELSE ROUND(((dt."ADR" - dt."YestDayADR") * 100.0 / dt."YestDayADR")::numeric, 2)
                          END AS "ADR Difference Perc",
                          sdf."Occupancy" AS "Forecasted Room Sold",
                          dt."OutOfOrder",
                          dt."Inventory",
                          dt."RoomSold",
                          CAST(dt."AvailableOccupancy" AS INTEGER) AS "Left To Sell",
                          ars."Highest_ADR_Past_8_Weeks",
                          ars."Lowest_ADR_Past_8_Weeks",
                          mb.max_rate,
                          mb.max_booking_date,
                          mb.max_booking_time,
                          lb.min_rate,
                          lb.min_booking_date,
                          lb.min_booking_time,
                          bar."BAR_Rate",
                          idx."Average_MPI",
                          idx."Average_ARI",
                          idx."Average_RGI",
                          ROUND(
                              100 - (
                                  ABS(1 - idx."Average_MPI" / NULLIF(idx."Average_ARI", 0)) +
                                  ABS(1 - idx."Average_ARI" / NULLIF(idx."Average_RGI", 0)) +
                                  ABS(1 - idx."Average_RGI" / NULLIF(idx."Average_MPI", 0))
                              ) * 20, 2
                          ) AS "Property_Score"
                      FROM dailydata_transaction dt
                      LEFT JOIN snp_dbd_forecast sdf ON dt."Dates" = sdf."Date"
                      LEFT JOIN latest_date ld ON dt."AsOfDate" = ld.asofdate
                      LEFT JOIN adr_rolling_stats ars ON TRUE
                      CROSS JOIN max_bookings mb
                      CROSS JOIN min_bookings lb
                      CROSS JOIN index_avg idx
                      CROSS JOIN bar_rate_avg bar
                      WHERE dt."AsOfDate" = ld.asofdate
                        AND dt."Dates" = dt."AsOfDate"
                        AND sdf."AsOfDate" = dt."AsOfDate";
              """
        daily_performance_dashboard_json = fetch_data(conn, daily_performance_dashboard_query)

        adr_by_bookingdate_query = f"""
                            WITH booking_days AS (
          SELECT DISTINCT "BookingDate"::date AS asof
          FROM copy_mst_reservation 
          WHERE "StayDate" = :stay_date
            AND "Status" IN ('R','O','I')
            AND "propertyCode" = :property_code
        )
        SELECT
          dt."AsOfDate"::date AS "Booking_Date",
          ROUND(dt."ADR", 0)  AS "ADR",
          dt."RoomSold"
        FROM dailydata_transaction dt
        JOIN booking_days b
          ON b.asof = dt."AsOfDate"::date
        WHERE dt."Dates"::date  = :stay_date
          AND dt."propertyCode" = :property_code
        ORDER BY dt."AsOfDate";
               """
        adr_by_bookingdate_json = fetch_data(conn, adr_by_bookingdate_query,{"property_code": PROPERTY_CODE, "stay_date": AS_OF_DATE})

        top10marketsegment_drilldown_query = f"""
          SELECT
                          cmr."MarketSegment",
                          COUNT(cmr."RoomNight") AS "Rooms",
                          round(SUM(cmr."Rate")) AS "Revenue",
                          CASE
                              WHEN COUNT(cmr."RoomNight") <> 0
                              THEN ROUND(SUM(cmr."Rate") / COUNT(cmr."RoomNight"))
                              ELSE 0
                          END AS "ADR"
                      FROM copy_mst_reservation cmr
                      WHERE
                          "propertyCode" = '{PROPERTY_CODE}'
                          AND "AsOfDate" = '{AS_OF_DATE}'
                          AND "StayDate" BETWEEN '{start_date}' AND '{end_date}'
                          and "Status" in ('I','O','R')
                      GROUP BY cmr."MarketSegment"
                      ORDER BY COUNT(*) DESC
                      LIMIT 10;
                      """
        top10marketsegment_drilldown_json = fetch_data(conn, top10marketsegment_drilldown_query)

        dashboard_revglance_query = f"""
         DO $$
      declare
          propertycode text := '{PROPERTY_CODE}';
          start_date DATE := '{start_date}';
          end_date DATE := '{end_date}';
          asofdate DATE := '{AS_OF_DATE}';
          id int := (select propertyid from rev_rmsproperty where "propertyCode" = '{PROPERTY_CODE}' order by propertyid  limit 1);
      begin
       
          DROP TABLE IF EXISTS temp_dashboard_revglance;  
          CREATE temp TABLE temp_dashboard_revglance AS
            select
              0 as "Inventory",
              0 as "OutOfOrder",
              0 as "RoomsOfAvailable",
              0 as "AvailableOccupancy",
              0 as "RoomSold",
              0 as "OCC",
              0 as "TotalRevenue",
              0 as "ADR",
              0 as "RevPAR",
              0 as "Pickup1RoomSold",
              0 as "Pickup1TotalRevenue",
              0 as "Pickup1ADR",
              0 as "Pickup7RoomSold",
              0 as "Pickup7TotalRevenue",
              0 as "Pickup7ADR",
              0 as "Pickup14RoomSold",
              0 as "Pickup14TotalRevenue",
              0 as "Pickup14ADR",
              0 as "Forecast_RoomSold",
              0 as "Forecast_ADR",
              0 as "Forecast_Revenue",
              0 as "NOVA_Forecast_RoomSold",
              0 as "NOVA_Forecast_ADR",
              0 as "NOVA_Forecast_Revenue",
              0 as "USER_Forecast_RoomSold",
              0 as "USER_Forecast_ADR",
              0 as "USER_Forecast_Revenue";
           
        update temp_dashboard_revglance set
            "Inventory" = subquery."Inventory",
            "OutOfOrder" = subquery."OutOfOrder",
            "RoomsOfAvailable" = subquery."RoomsOfAvailable",
            "AvailableOccupancy" = subquery."AvailableOccupancy",
            "RoomSold" = subquery."RoomSold",
            "OCC" = subquery."OCC",
            "TotalRevenue" = subquery."TotalRevenue",
            "ADR" = subquery."ADR",
            "RevPAR" = subquery."RevPAR",
            "Pickup1RoomSold" = subquery."Pickup1RoomSold",
            "Pickup1TotalRevenue" = subquery."Pickup1TotalRevenue",
            "Pickup1ADR" = subquery."Pickup1ADR",
            "Pickup7RoomSold" = subquery."Pickup7RoomSold",
            "Pickup7TotalRevenue" = subquery."Pickup7TotalRevenue",
            "Pickup7ADR" = subquery."Pickup7ADR",
            "Pickup14RoomSold" = subquery."Pickup14RoomSold",
            "Pickup14TotalRevenue" = subquery."Pickup14TotalRevenue",
            "Pickup14ADR" = subquery."Pickup14ADR"
        from (
            SELECT
              SUM(dt."Inventory") FILTER (WHERE dt."AsOfDate" = asofdate) AS "Inventory",
              ROUND(SUM(dt."OutOfOrder") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "OutOfOrder",
              ROUND(SUM(dt."RoomsOfAvailable") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "RoomsOfAvailable",
              ROUND(SUM(dt."AvailableOccupancy") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "AvailableOccupancy",
              ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "RoomSold",
              ROUND(AVG(dt."Occperc") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "OCC",
              ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "TotalRevenue",
              ROUND(AVG(dt."ADR") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "ADR",
              ROUND(AVG(dt."RevPAR") FILTER (WHERE dt."AsOfDate" = asofdate)) AS "RevPAR",
              COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
              COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '1 day')), 0) AS "Pickup1RoomSold",
              COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
              COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '7 day')), 0) AS "Pickup7RoomSold",
              COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
              COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '14 day')), 0) AS "Pickup14RoomSold",
              COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
              COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '1 day')), 0) AS "Pickup1TotalRevenue",
              COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
              COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '7 day')), 0) AS "Pickup7TotalRevenue",
              COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
              COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '14 day')), 0) AS "Pickup14TotalRevenue",
              ROUND(
                CASE
                  WHEN
                    COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                    COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '1 day')), 0)
                    <> 0
                  THEN
                    (
                      COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                      COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '1 day')), 0)
                    ) /
                    (
                      COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                      COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '1 day')), 0)
                    )
                  ELSE 0
                END
                ) AS "Pickup1ADR",
                      ROUND(
                CASE
                  WHEN
                    COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                    COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '7 day')), 0)
                    <> 0
                  THEN
                    (
                      COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                      COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '7 day')), 0)
                    ) /
                    (
                      COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                      COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '7 day')), 0)
                    )
                  ELSE 0
                END
                ) AS "Pickup7ADR",
                      ROUND(
                CASE
                  WHEN
                    COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                    COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '14 day')), 0)
                    <> 0
                  THEN
                    (
                      COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                      COALESCE(ROUND(SUM(dt."TotalRevenue") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '14 day')), 0)
                    ) /
                    (
                      COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate)), 0) -
                      COALESCE(ROUND(SUM(dt."RoomSold") FILTER (WHERE dt."AsOfDate" = asofdate::date - interval '14 day')), 0)
                    )
                  ELSE 0
                END
                ) AS "Pickup14ADR"
          FROM
              dailydata_transaction dt
          WHERE
              dt."propertyCode" = propertycode
              AND dt."AsOfDate" IN (
                            asofdate,
                            asofdate::date - interval '1 day',
                            asofdate::date - interval '7 day',
                            asofdate::date - interval '14 day'
                        )
              and "Dates" between start_date and end_date
        ) subquery;
 
 
          DROP TABLE IF EXISTS temp_intermediate;
          CREATE TEMP TABLE temp_intermediate AS
          SELECT
              "AsOfDate",
              "Dates",
              "RoomSold",
          "TotalRevenue",
              ROUND("ADR") AS "ADR"
          FROM
              dailydata_transaction
          WHERE
              "propertyCode" = propertycode
              AND "AsOfDate" = asofdate
              AND "Dates" BETWEEN start_date AND end_date;
 
 
      --   Update System Forecast ------------------------------------
 
          DROP TABLE IF EXISTS temp_intermediate_dbd_forecast;
          CREATE TEMP TABLE temp_intermediate_dbd_forecast AS
          SELECT
              "AsOfDate",
              "Date",
              "Occupancy",
          "Revenue",
              ROUND("Rate") AS "Rate"
          FROM
              snp_dbd_forecast
          WHERE
              "propertyCode" = propertycode
              AND "AsOfDate" = asofdate
              AND "Date" BETWEEN start_date AND end_date;
 
        DROP TABLE IF EXISTS temp_combined_system_forecast;
        CREATE TEMP TABLE temp_combined_system_forecast AS
        SELECT
            COALESCE(
                CASE WHEN di."Dates" < asofdate THEN di."RoomSold"
                    WHEN fi."Date" >= asofdate THEN fi."Occupancy"
                END, 0
            ) AS "Roomsold",
            COALESCE(
                CASE WHEN di."Dates" < asofdate THEN di."TotalRevenue"
                    WHEN fi."Date" >= asofdate THEN fi."Revenue"
                END, 0
            ) AS "Revenue",
            COALESCE(
                CASE WHEN di."Dates" < asofdate THEN di."ADR"
                    WHEN fi."Date" >= asofdate THEN fi."Rate"
                END, 0
            ) AS "ADR"
        FROM temp_intermediate di
        FULL OUTER JOIN temp_intermediate_dbd_forecast fi
        ON di."Dates" = fi."Date";
 
 
        update temp_dashboard_revglance set
            "Forecast_RoomSold" = subquery."Roomsold",
            "Forecast_ADR" = subquery."ADR",
            "Forecast_Revenue" = subquery."Revenue"
        from (
              SELECT
                sum("Roomsold") as "Roomsold",
              round(sum("Revenue")) as "Revenue",
              round(avg("ADR")) as "ADR"
            FROM
                temp_combined_system_forecast
        ) subquery;
 
 
      --   Update Nova Forecast ------------------------------------
 
        DROP TABLE IF EXISTS temp_intermediate_new_dbd_forecast;
          create temp table temp_intermediate_new_dbd_forecast as (
          select
              "AsOfDate",
              "Dates" ,
              "Predicted_RoomSold" ,
          "Predicted_Revenue",
              round("Predicted_Rate") as "Predicted_Rate"
          from
              new_dbd_forecast
          where
              new_dbd_forecast."propertyCode" = propertycode
              and "AsOfDate" = (select max("AsOfDate") from new_dbd_forecast ndf2 )
              and new_dbd_forecast."Dates" between start_date and end_date
          ) ;
 
 
        DROP TABLE IF EXISTS temp_combined_nova_forecast;
        CREATE TEMP TABLE temp_combined_nova_forecast AS
        SELECT
            COALESCE(
                CASE WHEN di."Dates" < asofdate THEN di."RoomSold"
                    WHEN fi."Dates" >= asofdate THEN fi."Predicted_RoomSold"
                END, 0
            ) AS "Roomsold",
            COALESCE(
                CASE WHEN di."Dates" < asofdate THEN di."TotalRevenue"
                    WHEN fi."Dates" >= asofdate THEN fi."Predicted_Revenue"
                END, 0
            ) AS "Revenue",
            COALESCE(
                CASE WHEN di."Dates" < asofdate THEN di."ADR"
                    WHEN fi."Dates" >= asofdate THEN fi."Predicted_Rate"
                END, 0
            ) AS "ADR"
        FROM temp_intermediate di
        FULL OUTER JOIN temp_intermediate_new_dbd_forecast fi
        ON di."Dates" = fi."Dates";
 
 
        update temp_dashboard_revglance set
            "NOVA_Forecast_RoomSold" = subquery."Roomsold",
            "NOVA_Forecast_ADR" = subquery."ADR",
            "NOVA_Forecast_Revenue" = subquery."Revenue"
        from (
              SELECT
                sum("Roomsold") as "Roomsold",
              round(sum("Revenue")) as "Revenue",
              round(avg("ADR")) as "ADR"
            FROM
                temp_combined_nova_forecast
        ) subquery;
 
 
      --   Update User Forecast -------------------------------------------------
 
        DROP TABLE IF EXISTS temp_intermediate_user_forecast;
          create temp table temp_intermediate_user_forecast as
          SELECT
          rf."forecastdate",
            COALESCE(rf."actualrevenue", 0) AS "Revenue",
          COALESCE(rf."actualoccupency", 0) AS "Roomsold",
          CASE
                  WHEN COALESCE(rf."actualoccupency", 0) = 0 THEN 0
                  ELSE rf."actualrevenue" / rf."actualoccupency"
              END AS "ADR"
        FROM
        (
            SELECT DISTINCT ON ("forecastdate")
            "forecastdate",
                  "actualrevenue",
                  "actualoccupency"
            FROM rev_forecast
            WHERE
                "propertyid" = id
                AND "forecasttype_term" = 'User Forecast'
                AND "forecastdate" BETWEEN start_date and end_date
            ORDER BY "forecastdate"
        ) rf;
 
 
        update temp_dashboard_revglance set
            "USER_Forecast_RoomSold" = subquery."Roomsold",
            "USER_Forecast_ADR" = subquery."ADR",
            "USER_Forecast_Revenue" = subquery."Revenue"
        from (
              SELECT
                coalesce(sum("Roomsold"),0) as "Roomsold",
              coalesce(round(sum("Revenue")),0) as "Revenue",
              coalesce(round(avg("ADR")),0) as "ADR"
            FROM
                temp_intermediate_user_forecast
        ) subquery;
 
      END $$;
         
          select
            *
          from
            temp_dashboard_revglance;
            """
        dashboard_revglance_json = fetch_data(conn, dashboard_revglance_query)
        
        marketsegment_mix_chart_query = f"""
          select
                          TO_CHAR(cmr."AsOfDate", 'yyyy-mm-dd') as "Date",
                          cmr."MarketSegment",
                          sum(cmr."RoomNight") as "Rooms Sold" ,
                          ROUND(AVG(cast(cmr."ADR" as numeric))) as "Average ADR"
                        from
                          copy_mst_reservation cmr
                        where
                          cmr."AsOfDate" = (
                          select
                            MAX("AsOfDate")
                          from
                            dailydata_transaction)
                          and cmr."StayDate" = cmr."AsOfDate" 
                          and cmr."Status" in ('I','R','O')
                        group by
                          cmr."MarketSegment",
                          cmr."AsOfDate" 
                        order by
                          "Rooms Sold" desc;
                          """
        marketsegment_mix_chart_json = fetch_data(conn, marketsegment_mix_chart_query)

        #Define one day pickup threshold
        odpt = 0
        one_day_pickup_threshold_query = f"""
              select
          distinct CAST(dt."Dates" AS TEXT) as  "Dates",
          coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
          coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '1 day'),0) as "RoomSold" ,
          coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
          coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '1 day'),0) as "Revenue" ,
          case
            when coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
              coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '1 day'),0) <> 0
            then round((coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
                  coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '1 day'),0))
                  /
                  (coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
                  coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '1 day'),0)))
            else 0
          end as "ADR",
          coalesce(
            CASE
              WHEN '{odpt}' IS NOT NULL AND '{odpt}' <> '' AND '{odpt}' <> 0 THEN '{odpt}'::INT
              ELSE (
                select
                  (rulevalues::jsonb->0->>'min')::INT
                from
                  rev_propertynotificationconfig rp
                where
                  rp.ruletype = 'Oneday Pickup Threshold'
              )
            END,
            0
          ) as threshold
        from
          dailydata_transaction dt
        where
          "AsOfDate" in ('{AS_OF_DATE}','{AS_OF_DATE}'::date - interval '1 day')
          and "Dates" >= '{AS_OF_DATE}'::date - interval '1 day'
          and "propertyCode" = '{PROPERTY_CODE}'
        group by CAST(dt."Dates" AS TEXT)
        having
          coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
          coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '1 day'),0)
          > coalesce(
          CASE
            WHEN '{odpt}' IS NOT NULL AND '{odpt}' <> '' AND '{odpt}' <> 0 THEN '{odpt}'::INT
            ELSE (
              select
                (rulevalues::jsonb->0->>'min')::INT
              from
                rev_propertynotificationconfig rp
              where
                rp.ruletype = 'Oneday Pickup Threshold'
            )
          END,
          0
        )
        order by CAST(dt."Dates" AS TEXT) ;
        """
        one_day_pickup_threshold_json = fetch_data(conn, one_day_pickup_threshold_query)

        candle_chart_query = f"""
            WITH date_bounds AS (
                        SELECT
                            MAX("AsOfDate") AS asofdate,
                            DATE '{start_date}' AS start_date, DATE '{end_date}' as end_date
                        FROM dailydata_transaction dt
                    ),
                    ranked_rates AS (
                        SELECT
                            cmr."StayDate",
                            cmr."Rate",
                            cmr."BookingDate",
                            cmr."BookingTime",
                            dt."ADR",
                            dt."LYPaceADR",
                            ROW_NUMBER() OVER (
                                PARTITION BY cmr."StayDate"
                                ORDER BY cmr."BookingDate", cmr."BookingTime"
                            ) AS open_rank,
                            ROW_NUMBER() OVER (
                                PARTITION BY cmr."StayDate"
                                ORDER BY cmr."BookingDate" DESC, cmr."BookingTime" DESC
                            ) AS close_rank
                        FROM copy_mst_reservation cmr
                        inner JOIN dailydata_transaction dt
                            ON cmr."StayDate" = dt."Dates"
                        inner JOIN date_bounds db
                            ON cmr."AsOfDate" = db.asofdate AND dt."AsOfDate" = db.asofdate
                        WHERE
                            cmr."StayDate" BETWEEN db.start_date AND db.end_date
                            AND cmr."Status" IN ('I', 'R', 'O')
                    ),
                    aggregated_rates AS (
                        SELECT
                            "StayDate",
                            MIN("Rate") AS "LowRate",
                            MAX("Rate") AS "HighRate",
                            COUNT(*) AS "RoomSold"
                        FROM ranked_rates
                        where "Rate" > 0
                        GROUP BY "StayDate"
                    ),
                    open_close AS (
                        SELECT
                            "StayDate",
                            MAX(CASE WHEN open_rank = 1 THEN "Rate" END) AS "OpeningRate",
                            MAX(CASE WHEN close_rank = 1 THEN "Rate" END) AS "ClosingRate",
                            MAX("LYPaceADR") AS "LYPaceADR",
                            MAX("ADR") AS "ADR"
                        FROM ranked_rates
                        GROUP BY "StayDate"
                    )
                    SELECT
                        TO_CHAR(oc."StayDate",'yyyy-mm-dd') as "StayDate",
                        TRIM(TO_CHAR(oc."StayDate", 'Day')) AS "Weekday",
                        ar."RoomSold",
                        oc."OpeningRate",
                        oc."ClosingRate",
                        ar."LowRate",
                        ar."HighRate",
                        oc."ADR",
                        oc."LYPaceADR" as "STLY_ADR"
                    FROM open_close oc
                    JOIN aggregated_rates ar
                        ON oc."StayDate" = ar."StayDate"
                    ORDER BY oc."StayDate";
                    """
        candle_chart_json = fetch_data(conn, candle_chart_query)

        comp_rate_variance_with_occ_self_query = f"""
          SELECT 
                to_char(comprates."AsOfDate",'yyyy-mm-dd') as "AsOfDate",
                to_char(comprates."CheckInDate",'yyyy-mm-dd') as "CheckInDate",
                comprates.propertyid,
                comprates.propertycompetiterid,
                comprates.competiterpropertyname,
                comprates."Channel",
                round(ddt."Occperc") as "Occperc",
                round(ddt."ADR") as "ADR",
                round(selfrates."Rate") AS "SelfRate",
                round(comprates."PreviousCompRate") as "PreviousCompRate",
                round(comprates."CurrentCompRate") as "CurrentCompRate",
                round(comprates."RateDifference") as "RateDifference",
                round(comprates."PercentageChange" * 100) as "PercentageChange"
            FROM (
                SELECT
                    rhrs1."AsOfDate" AS "AsOfDate",
                    rhrs1."CheckInDate",
                    rp.propertyid,
                    rp.propertycompetiterid,
                    rp.competiterpropertyname,
                    rhrs1."Channel",
                    rhrs2."Rate" AS "PreviousCompRate",
                    rhrs1."Rate" AS "CurrentCompRate",
                    rhrs1."Rate" - rhrs2."Rate" AS "RateDifference",
                    COALESCE(
                        CASE 
                            WHEN rhrs2."Rate" = 0 OR rhrs2."Rate" IS NULL THEN 1
                            ELSE ROUND((rhrs1."Rate" - rhrs2."Rate") / rhrs2."Rate", 2)
                        END, 
                        1
                    ) AS "PercentageChange"
                FROM
                    rs_history_rate_shop rhrs1
                LEFT JOIN 
                    rev_propertycompetiters rp 
                ON
                    CAST(rhrs1."CompetitorID" AS TEXT) = rp.competiterpropertycode
                LEFT JOIN 
                    rs_history_rate_shop rhrs2
                ON
                    rhrs1."CheckInDate" = rhrs2."CheckInDate"
                    AND rhrs1."Channel" = rhrs2."Channel"
                    AND rhrs1."CompetitorID" = rhrs2."CompetitorID"
                    AND rhrs2."AsOfDate" = (
                        SELECT MAX("AsOfDate") - INTERVAL '1 DAY' FROM rs_history_rate_shop rhrs
                    )
                WHERE
                    rhrs1."IsSelf" IS NOT TRUE
                    AND rhrs1."Channel" IN (
                        SELECT DISTINCT(rr.channel_term) 
                        FROM rev_rateshopconfig rr 
                        JOIN rev_propertycompetiters rp 
                        ON rr.propertyid = rp.propertyid
                    )
                    AND rhrs1."AsOfDate" = (
                        SELECT MAX("AsOfDate") FROM rs_history_rate_shop rhrs
                    )
                    AND rhrs1."CheckInDate" between rhrs1."AsOfDate" and rhrs1."AsOfDate" + interval '60 days'
                ORDER BY
                    rhrs1."CheckInDate",
                    rp.propertycompetiterid
            ) comprates
            LEFT JOIN rs_history_rate_shop selfrates
            ON 
                comprates."CheckInDate" = selfrates."CheckInDate"
                AND comprates."AsOfDate" = selfrates."AsOfDate"
                AND comprates."Channel" = selfrates."Channel"
                AND selfrates."IsSelf" IS true
            LEFT JOIN dailydata_transaction ddt
            ON	
                comprates."CheckInDate" = ddt."Dates"
                AND comprates."AsOfDate" = ddt."AsOfDate"
            WHERE
                comprates."CurrentCompRate" <> comprates."PreviousCompRate"
                and abs(comprates."PercentageChange" * 100) > 10
            ORDER BY comprates."CheckInDate";"""
        comp_rate_variance_with_occ_self_json = fetch_data(conn, comp_rate_variance_with_occ_self_query)

        lowDemandDates_query = f"""
                  DO $$
                  declare
                            propertycode text := '{PROPERTY_CODE}';
                            asofdate DATE := (SELECT MAX("AsOfDate") FROM dailydata_transaction dt);
                            sevendaypickup DATE := asofdate - interval '7 day';
                            onedaypickup DATE := asofdate - interval '1 day';
                            channel text := (select channel_term from rev_rateshopconfig where propertyid = (select propertyid from rev_rmsproperty where "propertyCode" = '{PROPERTY_CODE}' order by propertyid limit 1) limit 1);
                        begin


                DROP TABLE IF EXISTS dailydata_transaction_selection;
                  CREATE TEMP TABLE dailydata_transaction_selection AS
                select * from dailydata_transaction WHERE "propertyCode" = propertycode AND "AsOfDate" = asofdate;

                DROP TABLE IF EXISTS copy_mst_reservation_selection;
                  CREATE TEMP TABLE copy_mst_reservation_selection AS
                select * from copy_mst_reservation WHERE "AsOfDate" = asofdate;
                      
                      
                        DROP TABLE IF EXISTS temp_low_demand_dates;
                          CREATE TEMP TABLE temp_low_demand_dates AS
                        WITH CurrentData AS (
                            SELECT 
                                dt."AsOfDate" AS "CurrentAsOfDate",
                                dt."Dates" AS "StayDate",
                                dt."Occperc" AS "CurrentOccperc",
                                dt."ADR" as "ADR"
                            FROM dailydata_transaction_selection dt
                            WHERE dt."AsOfDate" = asofdate
                              AND dt."Dates" >= asofdate
                        ),
                        ForecastData AS (
                            SELECT 
                                sdf."AsOfDate" AS "ForecastAsOfDate",
                                sdf."Date" AS "StayDate",
                                sdf."Date" - sdf."AsOfDate" as "DBA",
                                sdf."Occperc" AS "ForecastedOccperc"
                            FROM snp_dbd_forecast sdf
                            WHERE sdf."AsOfDate" = asofdate
                              AND sdf."Date" >= asofdate
                        ),
                        AverageLeadtime AS (
                            SELECT 
                                cmr."StayDate",
                                ROUND(AVG(cmr."LeadTime"), 0) AS "LYAverageleadtime",
                                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY cmr."LeadTime" desc) AS "LY3QuartileLeadtime"
                            FROM copy_mst_reservation_selection cmr 
                            WHERE 
                                cmr."AsOfDate" = asofdate
                                AND cmr."StayDate" >= asofdate - INTERVAL '1 year'
                            GROUP BY cmr."StayDate"
                        )
                        SELECT 
                            CAST(fd."StayDate" AS TEXT),
                            TO_CHAR(fd."StayDate", 'Dy') AS "DayOfWeek",
                            fd."StayDate" - cd."CurrentAsOfDate" as "DaysBeforeArrival",
                            cd."ADR",
                            cd."CurrentOccperc",
                            fd."ForecastedOccperc",
                            round(al."LY3QuartileLeadtime") as "LYMedianLeadtime",
                            round(al."LY3QuartileLeadtime") as "LY3QuartileLeadtime",
                            0 as "Bar_Based_OTB",
                            0 as "Bar_Based_8week_AVG",
                            0 as "Forecast_RMS",
                            0 as "R28AVG",
                            0 as "Optimal_BAR",
                            null as "OnTheBook",
                              null as "1 Day Pickup",
                              null as "7 Day Pickup",
                              null as "Rateshop"
                        FROM ForecastData fd
                        LEFT JOIN CurrentData cd ON fd."StayDate" = cd."StayDate"
                        LEFT JOIN AverageLeadtime al ON fd."StayDate" = al."StayDate" + INTERVAL '1 year'
                        WHERE cd."CurrentOccperc" < (fd."ForecastedOccperc" / 2)
                          and fd."StayDate" - fd."ForecastAsOfDate" < al."LY3QuartileLeadtime"
                          and (fd."StayDate" - fd."ForecastAsOfDate") < 30
                        ORDER BY fd."StayDate" asc
                        limit 10;

                              
              
                        
                        DROP TABLE IF EXISTS temp_intermediate;
                        create temp table temp_intermediate as 
                              select
                                  "AsOfDate",
                                "StayDate",
                                sum("RoomNight") as "OTB"
                              from
                                copy_mst_reservation_selection
                              where
                                "propertyCode" = propertycode
                                and "AsOfDate" = asofdate
                                and "BarBased" = 'Y'
                                and "Pace" = 'PACE'
                                and "Status" in('I', 'R', 'O')
                              group by
                                "AsOfDate" ,
                                "StayDate";

                update temp_low_demand_dates set "Bar_Based_OTB" = (
                                      select
                                        sum("OTB") 
                                      from
                                        temp_intermediate
                                      where
                                        temp_low_demand_dates."StayDate"::date = temp_intermediate."StayDate"::date
                                    );

                update temp_low_demand_dates set "Bar_Based_8week_AVG" = (
                                                select
                                                  round(avg("OTB"))
                                                from
                                                      temp_intermediate
                                                where
                                                  temp_intermediate."StayDate" in (
                                                            (temp_low_demand_dates."StayDate"::date - interval '7 days') , 
                                                            (temp_low_demand_dates."StayDate"::date - interval '14 days') , 
                                                            (temp_low_demand_dates."StayDate"::date - interval '21 days') ,
                                                            (temp_low_demand_dates."StayDate"::date - interval '28 days'),
                                                            (temp_low_demand_dates."StayDate"::date - interval '35 days'),
                                                            (temp_low_demand_dates."StayDate"::date - interval '42 days'),
                                                            (temp_low_demand_dates."StayDate"::date - interval '49 days'),
                                                            (temp_low_demand_dates."StayDate"::date - interval '56 days')
                                                    )
                                                    limit 1
                                              );



                      DROP TABLE IF EXISTS temp_intermediate;
                          create temp table temp_intermediate as 
                            select
                                "AsOfDate",
                                "Dates",
                                "RoomSold",
                                round("ADR") as "ADR"
                            from
                                dailydata_transaction_selection
                            where
                                dailydata_transaction_selection."propertyCode" = propertycode 
                                and dailydata_transaction_selection."AsOfDate" = asofdate ;
                                    
                    
                          DROP TABLE IF EXISTS temp_intermediate2;
                          create temp table temp_intermediate2 as 
                          select
                            "AsOfDate",
                            "Date" ,
                            "Occupancy" ,
                            round("Rate") as "Rate"
                          from
                            snp_dbd_forecast
                          where
                            snp_dbd_forecast."propertyCode" = propertycode 
                            and snp_dbd_forecast."AsOfDate" = asofdate ;



                update temp_low_demand_dates set "Forecast_RMS" = (select temp_intermediate."RoomSold"
                                            
                                          from
                                            temp_intermediate
                                          where
                                            temp_intermediate."Dates"::date = temp_low_demand_dates."StayDate"::date limit 1
                                          )
                          where "StayDate"::date < asofdate;
                          
                update temp_low_demand_dates set "Forecast_RMS" = (select temp_intermediate2."Occupancy" 
                                              
                                            from
                                              temp_intermediate2
                                            where
                                              temp_intermediate2."Date"::date = temp_low_demand_dates."StayDate"::date limit 1
                                          )
                          where "StayDate"::date >= asofdate;

                update temp_low_demand_dates set "Optimal_BAR" = (select temp_intermediate."ADR"
                                            
                                          from
                                            temp_intermediate
                                          where
                                            temp_intermediate."Dates"::date = temp_low_demand_dates."StayDate"::date limit 1
                                          )
                          where "StayDate"::date < asofdate;
                          
                update temp_low_demand_dates set "Optimal_BAR" = (select round(temp_intermediate2."Rate")
                                              
                                            from
                                              temp_intermediate2
                                            where
                                              temp_intermediate2."Date"::date = temp_low_demand_dates."StayDate"::date limit 1
                                          )
                          where "StayDate"::date >= asofdate;


                update temp_low_demand_dates set "R28AVG" = (
                                            select
                                              round(avg("RoomSold"))
                                            from
                                                  dailydata_transaction_selection
                                            where
                                              dailydata_transaction_selection."AsOfDate" = asofdate
                                              and dailydata_transaction_selection."Dates" in (
                                                        (temp_low_demand_dates."StayDate"::date - interval '7 days') , 
                                                        (temp_low_demand_dates."StayDate"::date - interval '14 days') , 
                                                        (temp_low_demand_dates."StayDate"::date - interval '21 days') ,
                                                        (temp_low_demand_dates."StayDate"::date - interval '28 days')
                                                )
                                          )
                          where "StayDate"::date <= asofdate + interval '6 days';



                      DROP TABLE IF EXISTS temp_forcast_r28_after_asofdate_plus_day;   
                          CREATE temp TABLE temp_forcast_r28_after_asofdate_plus_day AS 
                          select
                          asofdate as "AsOfDate",
                            generate_series(
                                asofdate,
                                asofdate + interval '6 days',
                                interval '1 DAY'
                            )::date AS "Dates",0 as "R28AVG";
                            
                            
                            update temp_forcast_r28_after_asofdate_plus_day set "R28AVG" = (
                                            select
                                              round(avg("RoomSold"))
                                            from
                                                  dailydata_transaction_selection
                                            where
                                              dailydata_transaction_selection."AsOfDate" = asofdate
                                              and dailydata_transaction_selection."Dates" in (
                                                        (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '7 days') , 
                                                        (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '14 days') , 
                                                        (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '21 days') ,
                                                        (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '28 days')
                                                )
                                              
                                          )
                          where "Dates"::date <= asofdate + interval '6 days';
                        
                        
                      -- Remove this entire block or the line referencing the undefined table
                  UPDATE temp_low_demand_dates
                      SET "R28AVG" = temp_forcast_r28_after_asofdate_plus_day."R28AVG"
                      FROM temp_forcast_r28_after_asofdate_plus_day
                      WHERE to_char(temp_low_demand_dates."StayDate"::date , 'Day' ) = to_char(temp_forcast_r28_after_asofdate_plus_day."Dates" , 'Day' )
                      and temp_low_demand_dates."StayDate"::date > asofdate + interval '6 days';



                        DROP TABLE IF EXISTS temp_dailydata_transaction;   
                        CREATE temp TABLE temp_dailydata_transaction AS
                          select
                          "OutOfOrder" as "OOO",
                          cast("Dates" as text) as "Date",
                          "Inventory" as "RoomAvailable",
                          cast("AvailableOccupancy" as INTEGER) as "LeftToSell",
                          "RoomSold" as "OnTheBook",
                          cast("Occperc" as INTEGER) as "TotalOCCPercentage",
                          cast("ADR" as INTEGER),
                          cast("TotalRevenue" as INTEGER) as "REV",
                          CAST("RevPAR" as INTEGER),
                          "GroupOTB" AS "GroupOTB",
                                    "GroupBlock" AS "GroupBlock"
                        from
                          dailydata_transaction_selection dt
                        where
                          "AsOfDate" = asofdate
                          and "propertyCode" = propertycode
                          and "Dates" in (select "StayDate"::date from temp_low_demand_dates);

                UPDATE temp_low_demand_dates set "OnTheBook" = (
                                        select jsonb_agg(json_build_object(
                                          'OOO', dt."OOO",
                                                'RoomAvailable', dt."RoomAvailable",
                                                'LeftToSell', dt."LeftToSell",
                                                'OnTheBook', dt."OnTheBook",
                                                'TotalOCCPercentage', dt."TotalOCCPercentage",
                                                'ADR', dt."ADR",
                                                'REV', dt."REV",
                                                'RevPAR', dt."RevPAR",
                                                'OTB', dt."GroupOTB",
                                                'Block', dt."GroupBlock"
                                          ))
                                        from
                                          temp_dailydata_transaction dt
                                        where
                                          temp_low_demand_dates."StayDate"::date = dt."Date"::date
                                      );


                        DROP TABLE IF EXISTS temp_dailydata_transaction_1Pickup;   
                        CREATE temp TABLE temp_dailydata_transaction_1Pickup AS 
                        SELECT 
                            sub1."AsOfDate",
                            sub1."Dates",
                            (sub1."OTB1" - sub2."OTB2") AS "RMS",
                            CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2")) as INTEGER ) AS "REV",
                            CASE 
                                WHEN (sub1."OTB1" - sub2."OTB2") <> 0   AND (sub1."TotalRevenue1" - sub2."TotalRevenue2") <> 0 THEN
                                    CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2") / (sub1."OTB1" - sub2."OTB2"))   as INTEGER) 
                                ELSE
                                    0
                            END AS "ADR"
                        FROM (
                            SELECT 
                                CAST("AsOfDate" as TEXT),
                                CAST("Dates" as TEXT),
                                "RoomSold" as "OTB1",
                                "TotalRevenue" as "TotalRevenue1" 
                            FROM 
                              dailydata_transaction_selection
                            WHERE
                                "AsOfDate" = asofdate 
                                AND "propertyCode" = propertycode
                                AND "Dates" in (select "StayDate"::date from temp_low_demand_dates)
                        ) sub1
                        LEFT JOIN (
                            SELECT 
                                CAST("AsOfDate" as TEXT),
                                CAST("Dates" as TEXT),
                                "RoomSold" as "OTB2",
                                "TotalRevenue" as "TotalRevenue2" 
                            FROM 
                              dailydata_transaction
                            WHERE
                                "AsOfDate" = onedaypickup
                                AND "propertyCode" = propertycode
                                AND "Dates" in (select "StayDate"::date from temp_low_demand_dates)
                        ) sub2 ON sub1."Dates" = sub2."Dates";

                UPDATE temp_low_demand_dates set "1 Day Pickup" = (
                                        select jsonb_agg(json_build_object(
                                          'RMS', dtp."RMS",
                                                'REV', dtp."REV",
                                                'ADR', dtp."ADR"
                                          ))
                                        from
                                          temp_dailydata_transaction_1Pickup dtp
                                        where
                                          temp_low_demand_dates."StayDate"::date = dtp."Dates"::date
                                      );




                        DROP TABLE IF EXISTS temp_dailydata_transaction_7Pickup;   
                        CREATE temp TABLE temp_dailydata_transaction_7Pickup AS 
                        SELECT 
                            sub1."AsOfDate",
                            sub1."Dates",
                            (sub1."OTB1" - sub2."OTB2") AS "RMS",
                            CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2")) as INTEGER ) AS "REV",
                            CASE 
                                WHEN (sub1."OTB1" - sub2."OTB2") <> 0   AND (sub1."TotalRevenue1" - sub2."TotalRevenue2") <> 0 THEN
                                    CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2") / (sub1."OTB1" - sub2."OTB2"))   as INTEGER) 
                                ELSE
                                    0
                            END AS "ADR"
                        FROM (
                            SELECT 
                                CAST("AsOfDate" as TEXT),
                                CAST("Dates" as TEXT),
                                "RoomSold" as "OTB1",
                                "TotalRevenue" as "TotalRevenue1" 
                            FROM 
                              dailydata_transaction
                            WHERE
                                "AsOfDate" = asofdate 
                                AND "propertyCode" = propertycode
                                AND "Dates" in (select "StayDate"::date from temp_low_demand_dates)
                        ) sub1
                        LEFT JOIN (
                            SELECT 
                                CAST("AsOfDate" as TEXT),
                                CAST("Dates" as TEXT),
                                "RoomSold" as "OTB2",
                                "TotalRevenue" as "TotalRevenue2" 
                            FROM 
                              dailydata_transaction
                            WHERE
                                "AsOfDate" = sevendaypickup
                                AND "propertyCode" = propertycode
                                AND "Dates" in (select "StayDate"::date from temp_low_demand_dates)
                        ) sub2 ON sub1."Dates" = sub2."Dates";

                UPDATE temp_low_demand_dates set "7 Day Pickup" = (
                                        select jsonb_agg(json_build_object(
                                          'RMS', dtp."RMS",
                                                'REV', dtp."REV",
                                                'ADR', dtp."ADR"
                                          ))
                                        from
                                          temp_dailydata_transaction_7Pickup dtp
                                        where
                                          temp_low_demand_dates."StayDate"::date = dtp."Dates"::date
                                      );


                      DROP TABLE IF EXISTS temp_rs_history_rate_shop;   
                      CREATE temp TABLE temp_rs_history_rate_shop AS        
                      select 
                            rp.competiterpropertyname, 
                            trs.* 
                        from 
                            rs_history_rate_shop trs 
                        left join
                            rev_propertycompetiters rp 
                            on rp.competiterpropertycode = CAST(trs."CompetitorID" as Text) 
                        where
                            trs."PropertyCode" = propertycode 
                            and "AsOfDate" = asofdate
                            and "CheckInDate" in (select "StayDate"::date from temp_low_demand_dates)
                            and ("Channel" IS NULL OR "Channel" = channel)
                        order by 
                            trs."CheckInDate",
                            "competiterpropertyname";

                UPDATE temp_low_demand_dates set "Rateshop" = (
                                        select jsonb_agg(json_build_object(
                                          'competiterpropertyname', trs."competiterpropertyname",
                                          'DayOfWeek', trs."DayOfWeek",
                                          'CompetitorID', trs."CompetitorID",
                                          'Rate', trs."Rate",
                                          'Channel', trs."Channel",
                                          'LOS', trs."LOS",
                                          'RoomType', trs."RoomType",
                                          'IsSelf', trs."IsSelf",
                                          'IsLowestRate', trs."IsLowestRate"
                                          ))
                                        from
                                          temp_rs_history_rate_shop trs
                                        where
                                          temp_low_demand_dates."StayDate"::date = trs."CheckInDate"::date
                                      );
                          
                    END 
                  $$ LANGUAGE plpgsql;	        

                    select
                      *
                    from
                      temp_low_demand_dates
                    order by "StayDate";"""
        lowDemandDates_json = fetch_data(conn, lowDemandDates_query)

        forecast_mix_chart_query = f"""
            WITH latest_dt AS (
                            SELECT MAX("AsOfDate") AS max_date
                            FROM dailydata_transaction
                        ),
                        
                        target_year AS (
                            SELECT EXTRACT(YEAR FROM max_date)::INT AS year
                            FROM latest_dt
                        ),
                        
                        months AS (
                            SELECT * FROM (VALUES
                                ('January', 1), ('February', 2), ('March', 3),
                                ('April', 4), ('May', 5), ('June', 6),
                                ('July', 7), ('August', 8), ('September', 9),
                                ('October', 10), ('November', 11), ('December', 12)
                            ) AS m(name, num)
                        ),
                        
                        latest_user AS (
                            SELECT *
                            FROM snp_fc_user
                            WHERE "AsOfDate" = (SELECT MAX("AsOfDate") FROM snp_fc_user)
                        ),
                        
                        latest_spider AS (
                            SELECT *
                            FROM snp_fc_spider
                            WHERE "AsOfDate" = (SELECT MAX("AsOfDate") FROM snp_fc_spider)
                        ),
                        
                        latest_nova AS (
                            SELECT *
                            FROM snp_fc_nova
                            WHERE "AsOfDate" = (SELECT MAX("AsOfDate") FROM snp_fc_nova)
                        )
                        
                        SELECT
                            COALESCE(n."propertyCode", s."propertyCode", u."propertyCode") AS "propertyCode",
                            ty.year,
                            INITCAP(m.name) AS month,
                            m.num AS month_no,
                        
                            -- Raw Forecasts
                            s."occ" AS occ_spider,
                            u."occ" AS occ_user,
                            n."occ" AS occ_nova,
                        
                            s."adr" AS adr_spider,
                            u."adr" AS adr_user,
                            n."adr" AS adr_nova,
                        
                            s."rms" AS rms_spider,
                            u."rms" AS rms_user,
                            n."rms" AS rms_nova,
                        
                            s."rev" AS rev_spider,
                            u."rev" AS rev_user,
                            n."rev" AS rev_nova,
                        
                            -- OCC Aggregates
                            occ_stats.avg_occ,
                            occ_stats.max_occ,
                            occ_stats.min_occ,
                        
                            -- ADR Aggregates
                            adr_stats.avg_adr,
                            adr_stats.max_adr,
                            adr_stats.min_adr,
                        
                            -- RMS Aggregates
                            rms_stats.avg_rms,
                            rms_stats.max_rms,
                            rms_stats.min_rms,
                        
                            -- REV Aggregates
                            rev_stats.avg_rev,
                            rev_stats.max_rev,
                            rev_stats.min_rev
                        
                        FROM target_year ty
                        CROSS JOIN months m
                        
                        LEFT JOIN latest_spider s
                            ON s."year" = ty.year AND INITCAP(TRIM(s."month")) = m.name
                        
                        LEFT JOIN latest_user u
                            ON u."year" = ty.year AND INITCAP(TRIM(u."month")) = m.name
                        
                        LEFT JOIN latest_nova n
                            ON n."year" = ty.year AND INITCAP(TRIM(n."month")) = m.name
                        
                        -- OCC Aggregates
                        LEFT JOIN LATERAL (
                            SELECT
                                ROUND(AVG(val)::numeric, 2) AS avg_occ,
                                MAX(val) AS max_occ,
                                MIN(val) AS min_occ
                            FROM (
                                SELECT unnest(ARRAY[
                                    NULLIF(s."occ", 0),
                                    NULLIF(u."occ", 0),
                                    NULLIF(n."occ", 0)
                                ]) AS val
                            ) x
                            WHERE val IS NOT NULL
                        ) AS occ_stats ON true
                        
                        -- ADR Aggregates
                        LEFT JOIN LATERAL (
                            SELECT
                                ROUND(AVG(val)::numeric, 2) AS avg_adr,
                                MAX(val) AS max_adr,
                                MIN(val) AS min_adr
                            FROM (
                                SELECT unnest(ARRAY[
                                    NULLIF(s."adr", 0),
                                    NULLIF(u."adr", 0),
                                    NULLIF(n."adr", 0)
                                ]) AS val
                            ) x
                            WHERE val IS NOT NULL
                        ) AS adr_stats ON true
                        
                        -- RMS Aggregates
                        LEFT JOIN LATERAL (
                            SELECT
                                ROUND(AVG(val)::numeric, 2) AS avg_rms,
                                MAX(val) AS max_rms,
                                MIN(val) AS min_rms
                            FROM (
                                SELECT unnest(ARRAY[
                                    NULLIF(s."rms", 0),
                                    NULLIF(u."rms", 0),
                                    NULLIF(n."rms", 0)
                                ]) AS val
                            ) x
                            WHERE val IS NOT NULL
                        ) AS rms_stats ON true
                        
                        -- REV Aggregates
                        LEFT JOIN LATERAL (
                            SELECT
                                ROUND(AVG(val)::numeric, 2) AS avg_rev,
                                MAX(val) AS max_rev,
                                MIN(val) AS min_rev
                            FROM (
                                SELECT unnest(ARRAY[
                                    NULLIF(s."rev", 0),
                                    NULLIF(u."rev", 0),
                                    NULLIF(n."rev", 0)
                                ]) AS val
                            ) x
                            WHERE val IS NOT NULL
                        ) AS rev_stats ON true
                        
                        ORDER BY m.num;"""
        forecast_mix_chart_json = fetch_data(conn, forecast_mix_chart_query)

        booking_pace_comparison_chart_query = f"""
            DO $$
                DECLARE todaydate date = '{start_date}';
                DECLARE asofdate date = '{AS_OF_DATE}';
                DECLARE noofprevdays integer = 30; 	
                DECLARE noofnextdays integer = 31; 
                DECLARE noofDBAPU integer = 90;
                DECLARE noofDBAPUDayIntervel integer = 1;
                DECLARE propertyCode text = '{PROPERTY_CODE}';

                DECLARE cnt  integer = 0; 
                BEGIN

                      DROP TABLE IF EXISTS temp_date_table;
                      CREATE TEMP TABLE temp_date_table AS
                      select
                        generate_series(
                            todaydate - noofprevdays,
                            todaydate + noofnextdays,
                        interval '1 DAY'
                        )::date as "staydate",0 as day_diff,0 as total_booking,0 as LY_booking,0 as forcastroom, 0 as pu_avg,0 as dba_avg;

                        update temp_date_table set day_diff = (staydate-todaydate);


                        DROP TABLE IF EXISTS temp_pms_res;
                        CREATE TEMP TABLE temp_pms_res as
                        select "StayDate","BookingDate" from copy_mst_reservation where 
                        "propertyCode" = propertyCode
                        and "RateCode" not in ('GROUP~') 
                        and "Status" in ('R', 'O', 'I') 
                        and "StayDate":: date between (todaydate - noofprevdays)- interval '1 year' and todaydate + noofnextdays
                        and "AsOfDate"::date = asofdate;

                        update temp_date_table set total_booking = (select count(1) from temp_pms_res where temp_pms_res."StayDate"::date = temp_date_table."staydate");
                        update temp_date_table set LY_booking = (select count(1) from temp_pms_res where (temp_pms_res."StayDate"::date) = (temp_date_table."staydate")- interval '1 year');

                        DROP TABLE IF EXISTS temp_pms_res;	
                        CREATE TEMP TABLE temp_pms_res as
                        select "StayDate","BookingDate" from copy_mst_reservation where 
                        "propertyCode" = propertyCode 
                        and "RateCode" not in ('GROUP~') 
                        and "Status" in ('R', 'O', 'I') 
                        and "StayDate":: date between todaydate - noofprevdays-noofDBAPU and todaydate + noofnextdays
                        and "AsOfDate"::date = asofdate;



                        loop  
                        exit when cnt = noofDBAPU+noofDBAPUDayIntervel ;
                        EXECUTE (select concat ('alter table temp_date_table add column dba_',noofDBAPU-cnt,' int')) ;
                        EXECUTE (select concat ('alter table temp_date_table add column pu_',noofDBAPU-cnt,' int')) ;
                        EXECUTE (select concat('update temp_date_table set pu_',noofDBAPU-cnt,'=(select count(1) FROM temp_pms_res where temp_pms_res."StayDate":: date = temp_date_table.staydate  and temp_pms_res."BookingDate":: date >  temp_date_table.staydate - ',noofDBAPU-cnt,' )'));
                        EXECUTE (select concat('update temp_date_table set dba_',noofDBAPU-cnt,'=temp_date_table."total_booking" - pu_',noofDBAPU-cnt));


                        EXECUTE (select concat ('update temp_date_table set "forcastroom" = "total_booking" + (select AVG(pu_',noofDBAPU-cnt,') from temp_date_table T where T."day_diff" < 0 ) where day_diff between ',((noofDBAPU-cnt)-noofDBAPUDayIntervel),' and ',noofDBAPU-cnt,'')) ;


                        EXECUTE (select concat ('update temp_date_table set "pu_avg" = (select AVG(', 'pu_', noofDBAPU-cnt, ') from temp_date_table T where T."day_diff" < 0 ) where day_diff between ', ((noofDBAPU-cnt)-noofDBAPUDayIntervel), ' and ', noofDBAPU-cnt, ''));

                        EXECUTE (select concat ('update temp_date_table set "dba_avg" = (select AVG(', 'dba_', noofDBAPU-cnt, ') from temp_date_table T where T."day_diff" < 0 ) where day_diff between ', ((noofDBAPU-cnt)-noofDBAPUDayIntervel), ' and ', noofDBAPU-cnt, ''));

                        cnt := cnt + noofDBAPUDayIntervel ;  
                        end loop; 


                END $$;
              select
                "staydate",
                day_diff,
                total_booking,
                forcastroom,
                dba_avg as rsa,
                cast("staydate" as text),
                TRIM (
                      trailing
                  from
                  To_Char("staydate",
                'Day')
                  ) as "Weekday"
              from
                temp_date_table
              where 
                "staydate" between  '{start_date}' and '{end_date}'
              order by
                cast("staydate" as text);"""
        booking_pace_comparison_chart_json = fetch_data(conn, booking_pace_comparison_chart_query)

        sdpt = 0
        seven_day_pickup_threshold_query = f"""
                                        select
        distinct CAST(dt."Dates" AS TEXT) as  "Dates",
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '7 day'),0) as "RoomSold" ,
        coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
        coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '7 day'),0) as "Revenue" ,
        case
          when coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
            coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '7 day'),0) <> 0
          then round((coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
                coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '7 day'),0))
                /
                (coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
                coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '7 day'),0)))
          else 0
        end as "ADR",
        coalesce(
            CASE
              WHEN '{sdpt}' IS NOT NULL AND '{sdpt}' <> '' AND '{sdpt}' <> 0 THEN '{sdpt}'::INT
              ELSE (
                select
                  (rulevalues::jsonb->0->>'min')::INT
                from
                  rev_propertynotificationconfig rp
                where
                  rp.ruletype = 'Sevenday Pickup Threshold'
              )
            END,
            0
          ) as threshold
      from
        dailydata_transaction dt
      where
        "AsOfDate" in ('{AS_OF_DATE}','{AS_OF_DATE}'::date - interval '7 day')
        and "Dates" >= '{AS_OF_DATE}'::date - interval '7 day'
        and "propertyCode" = '{PROPERTY_CODE}'
      group by CAST(dt."Dates" AS TEXT)
      having
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '7 day'),0)
        > coalesce(
            CASE
              WHEN '{sdpt}' IS NOT NULL AND '{sdpt}' <> '' AND '{sdpt}' <> 0 THEN '{sdpt}'::INT
              ELSE (
                select
                  (rulevalues::jsonb->0->>'min')::INT
                from
                  rev_propertynotificationconfig rp
                where
                  rp.ruletype = 'Sevenday Pickup Threshold'
              )
            END,
            0
          )
      order by CAST(dt."Dates" AS TEXT) ;"""
        seven_day_pickup_threshold_json = fetch_data(conn, seven_day_pickup_threshold_query)

        highDemandDates_query = f"""
          DO $$
                declare
                    propertycode text := '{PROPERTY_CODE}';
                    asofdate DATE := (SELECT MAX("AsOfDate") FROM dailydata_transaction dt);
                    sevendaypickup DATE := asofdate - interval '7 day';
                    onedaypickup DATE := asofdate - interval '1 day';
                    channel text := (select channel_term from rev_rateshopconfig where propertyid = (select propertyid from rev_rmsproperty where "propertyCode" = '{PROPERTY_CODE}' order by propertyid  limit 1) limit 1);
                begin
              
                DROP TABLE IF EXISTS dailydata_transaction_selection;
              CREATE TEMP TABLE dailydata_transaction_selection AS
            select * from dailydata_transaction WHERE "propertyCode" = propertycode AND "AsOfDate" = asofdate;

            DROP TABLE IF EXISTS copy_mst_reservation_selection;
              CREATE TEMP TABLE copy_mst_reservation_selection AS
            select * from copy_mst_reservation WHERE "AsOfDate" = asofdate;

              
                DROP TABLE IF EXISTS temp_high_demand_dates;
                  CREATE TEMP TABLE temp_high_demand_dates AS
                      select 
                        CAST(t1."StayDate" AS TEXT), 
                        t1."DayOfWeek",
                        round(t1."Occperc") as "Occperc",
                        round(t1."OccuPercChange") as "OccuPercChange",
                        t1."Attendance",
                        t1."Event_Count",
                  t1."title",
                        (t1."Oscore" + t1."Cscore" + t1."Wscore" + t1."Escore") as "TotalScore",
                  t1."Event_details",
                  0 as "Bar_Based_OTB",
                  0 as "Bar_Based_8week_AVG",
                  0 as "Forecast_RMS",
                  0 as "R28AVG",
                  0 aS "Optimal_BAR",
                  null as "OnTheBook",
                    null as "1 Day Pickup",
                    null as "7 Day Pickup",
                    null as "Rateshop"
                      from 
                      (
                      WITH date_range AS (
                          SELECT
                              generate_series(
                                  asofdate,
                                  (SELECT date_trunc('year', MAX("AsOfDate")) + INTERVAL '2 years - 1 day' FROM dailydata_transaction dt),
                                  INTERVAL '1 day'
                              )::date AS "StayDate"
                      ),
                      occupancychange AS (
                          SELECT   
                              cmr."StayDate",
                              round(count(cmr.*) * 100.0 / NULLIF(dt2."Inventory", 0), 2) AS "OccupancyPercentage"
                          FROM
                              copy_mst_reservation_selection cmr
                          LEFT JOIN
                              dailydata_transaction dt2
                          ON
                              cmr."StayDate" = dt2."Dates"
                              AND cmr."AsOfDate" = dt2."AsOfDate"
                          WHERE
                              cmr."AsOfDate" = asofdate
                              AND cmr."StayDate" >= cmr."AsOfDate"
                              AND "BookingDate" BETWEEN asofdate - INTERVAL '7 days'
                                  AND asofdate
                          GROUP BY
                              cmr."StayDate",
                              dt2."Inventory"
                      ),
                      occupancy_data AS (
                          SELECT
                          "Dates" ,
                          "DayOfWeek" ,
                          "Occperc" 
                        FROM
                          dailydata_transaction_selection dt
                        WHERE
                          "propertyCode" = propertycode
                          AND "AsOfDate" = asofdate
                        order by "Dates" 
                      ),
                      Events AS (
                          WITH MinAttendence AS (
                          SELECT
                              round(round(AVG("Occperc")) * 
                              round(avg("Inventory")) * (SELECT count(*) FROM rev_propertycompetiters rp) / 100) AS min
                            FROM
                              dailydata_transaction_selection
                            WHERE
                              "AsOfDate" = asofdate
                              AND "propertyCode" = propertycode
                          )
                        SELECT
                          rae."propertyCode",
                          rae."AsOfDate",
                          to_char(gs.eventdate,'yyyy-mm-dd') as "Eventdate" ,
                          sum(coalesce(rae.phq_attendance, 0)) AS "Attendance",
                          count(*) as "Event_Count",
                  STRING_AGG(rae."title", ', ') AS "title",
                  (select jsonb_agg(json_build_object(
                                  'pullDateId', rae."pullDateId",
                                  'AsOfDate', rae."AsOfDate",
                                  'event_id', rae."event_id",
                                  'title', rae."title",
                                  'description', rae."description",
                                  'category', rae."category",
                                  'labels', rae."labels",
                                  'rank', rae."rank",
                                  'local_rank', rae."local_rank",
                                  'phq_attendance', rae."phq_attendance",
                                  'type', rae."type",
                                  'start_date', rae."start_date",
                                  'end_date', rae."end_date",
                                  'timezone', rae."timezone",
                                  'geo_lat', rae."geo_lat",
                                  'geo_long', rae."geo_long",
                                  'place_hierarchies', rae."place_hierarchies",
                                  'state', rae."state",
                                  'private', rae."private"
                                  )) ) as "Event_details",
                          'Events' as "Reason"
                        FROM
                          rev_events re join rev_all_events rae on re.eventsourceid = rae.event_id,
                          LATERAL generate_series(rae.start_date, rae.end_date, '1 day'::interval) AS gs(eventdate)
                        CROSS JOIN
                          MinAttendence
                            where re.isactive is true
                        GROUP BY 
                          rae."propertyCode",
                          rae."AsOfDate",
                          TO_CHAR(gs.eventdate, 'yyyy-mm-dd')
                        ORDER BY 
                          TO_CHAR(gs.eventdate, 'yyyy-mm-dd')
                      ),
                      DOWRank as (
                        SELECT
                            "DayOfWeek",
                            RANK() OVER (ORDER BY AVG("Occperc") DESC) AS "Ranking"
                        FROM
                            dailydata_transaction_selection dt
                        WHERE
                            "AsOfDate" = asofdate
                        GROUP BY
                            "DayOfWeek"
                        ORDER BY
                            "Ranking"
                      ),
                      MinAttendence AS (
                          SELECT
                              round(round(AVG("Occperc")) * 
                              round(avg("Inventory")) * (SELECT count(*) FROM rev_propertycompetiters rp) / 100) AS min
                            FROM
                              dailydata_transaction_selection
                            WHERE
                              "AsOfDate" = asofdate
                              AND "propertyCode" = propertycode
                      ),
                Threshold AS (
                    SELECT
                        round(avg("Occperc")) as "Occ_Threshold"
                    FROM
                        dailydata_transaction_selection
                    WHERE
                        "AsOfDate" = asofdate
                        AND "propertyCode" = propertycode
                        AND "Dates" >= (DATE_TRUNC('month', "AsOfDate"::date) - INTERVAL '1 month')::date
                        AND "Dates" <= (DATE_TRUNC('month', "AsOfDate"::date) - INTERVAL '1 day')::date
                )
                      SELECT
                        dr."StayDate",
                        od."Occperc",
                        COALESCE(oc."OccupancyPercentage", 0.0) AS "OccuPercChange",
                        od."DayOfWeek" ,
                        coalesce(e."Attendance",0) as "Attendance",
                        coalesce(e."Event_Count",0) as "Event_Count",
                  e."Event_details",
                  e."title",
                        CASE 
                    WHEN COALESCE(od."Occperc", 0) > Threshold."Occ_Threshold" THEN 
                        CASE 
                            WHEN COALESCE(od."Occperc", 0) >= 0 AND COALESCE(od."Occperc", 0) <= 10.0 THEN 5 
                            WHEN od."Occperc" > 10.0 AND od."Occperc" <= 20.0 THEN 10
                            WHEN od."Occperc" > 20.0 AND od."Occperc" <= 30.0 THEN 15
                            WHEN od."Occperc" > 30.0 AND od."Occperc" <= 40.0 THEN 20
                            WHEN od."Occperc" > 40.0 AND od."Occperc" <= 50.0 THEN 25
                            WHEN od."Occperc" > 50.0 AND od."Occperc" <= 60.0 THEN 30
                            WHEN od."Occperc" > 60.0 AND od."Occperc" <= 70.0 THEN 35
                            WHEN od."Occperc" > 70 THEN 40
                            ELSE 0 
                        END
                    ELSE 0
                  END AS "Oscore",
                  CASE 
                      WHEN COALESCE(od."Occperc", 0) > Threshold."Occ_Threshold" THEN 
                          CASE 
                              WHEN COALESCE(oc."OccupancyPercentage", 0) >= 0 AND COALESCE(oc."OccupancyPercentage", 0) <= 2.0 THEN 10
                            WHEN oc."OccupancyPercentage" > 2.0 AND oc."OccupancyPercentage" <= 4.0 THEN 15
                            WHEN oc."OccupancyPercentage" > 4.0 AND oc."OccupancyPercentage" <= 10.0 THEN 20
                            WHEN oc."OccupancyPercentage" > 10.0 AND oc."OccupancyPercentage" <= 15.0 THEN 25
                            WHEN oc."OccupancyPercentage" > 15 THEN 30
                              ELSE 0 
                          END
                      ELSE 0
                  END AS "Cscore",
                  CASE 
                      WHEN COALESCE(od."Occperc", 0) > Threshold."Occ_Threshold" THEN 
                          CASE 
                              WHEN r."Ranking" = 7 THEN 8
                            WHEN r."Ranking" = 6 THEN 10
                            WHEN r."Ranking" = 5 THEN 12
                            WHEN r."Ranking" = 4 THEN 14
                            WHEN r."Ranking" = 3 THEN 16
                            WHEN r."Ranking" = 2 THEN 18
                            WHEN r."Ranking" = 1 THEN 20
                              ELSE 0 
                          END
                      ELSE 0
                  END AS "Wscore",
                  CASE 
                      WHEN COALESCE(od."Occperc", 0) > Threshold."Occ_Threshold" THEN 
                          CASE 
                              WHEN COALESCE(e."Attendance", 0) >= 0 AND COALESCE(e."Attendance", 0) <= MinAttendence.min THEN 5
                            WHEN e."Attendance" > MinAttendence.min AND e."Attendance" <= 500 THEN 6
                            WHEN e."Attendance" > 500 AND e."Attendance" <= 700 THEN 7
                            WHEN e."Attendance" > 700 AND e."Attendance" <= 900 THEN 8
                            WHEN e."Attendance" > 900 AND e."Attendance" <= 1500 THEN 9
                            WHEN e."Attendance" > 1500 THEN 10
                              ELSE 0 
                          END
                      ELSE 0
                  END AS "Escore"
                      FROM
                          date_range dr
                        left join occupancychange oc on dr."StayDate" = oc."StayDate"
                        left join occupancy_data od on dr."StayDate" = od."Dates"
                        left join Events e on dr."StayDate" = e."Eventdate"::date
                        left join DOWRank r on od."DayOfWeek" = r."DayOfWeek"
                        CROSS JOIN
                          MinAttendence
                  cross join Threshold
                      ORDER BY
                          dr."StayDate" ) t1
                      where (t1."Oscore" + t1."Cscore" + t1."Wscore" + t1."Escore") <> 0
                order by (t1."Oscore" + t1."Cscore" + t1."Wscore" + t1."Escore") desc 
                      limit 10 ;

                
                DROP TABLE IF EXISTS temp_intermediate;
                create temp table temp_intermediate as 
                      select
                          "AsOfDate",
                        "StayDate",
                        sum("RoomNight") as "OTB"
                      from
                        copy_mst_reservation_selection
                      where
                        "propertyCode" = propertycode
                        and "AsOfDate" = asofdate
                        and "BarBased" = 'Y'
                        and "Pace" = 'PACE'
                        and "Status" in('I', 'R', 'O')
                      group by
                        "AsOfDate" ,
                        "StayDate";

        update temp_high_demand_dates set "Bar_Based_OTB" = (
                              select
                                sum("OTB") 
                              from
                                temp_intermediate
                              where
                                temp_high_demand_dates."StayDate"::date = temp_intermediate."StayDate"::date
                            );

        update temp_high_demand_dates set "Bar_Based_8week_AVG" = (
                                        select
                                          round(avg("OTB"))
                                        from
                                              temp_intermediate
                                        where
                                          temp_intermediate."StayDate" in (
                                                    (temp_high_demand_dates."StayDate"::date - interval '7 days') , 
                                                    (temp_high_demand_dates."StayDate"::date - interval '14 days') , 
                                                    (temp_high_demand_dates."StayDate"::date - interval '21 days') ,
                                                    (temp_high_demand_dates."StayDate"::date - interval '28 days'),
                                                    (temp_high_demand_dates."StayDate"::date - interval '35 days'),
                                                    (temp_high_demand_dates."StayDate"::date - interval '42 days'),
                                                    (temp_high_demand_dates."StayDate"::date - interval '49 days'),
                                                    (temp_high_demand_dates."StayDate"::date - interval '56 days')
                                            )
                                            limit 1
                                      );



              DROP TABLE IF EXISTS temp_intermediate;
                  create temp table temp_intermediate as 
                    select
                        "AsOfDate",
                        "Dates",
                        "RoomSold",
                        round("ADR") as "ADR"
                    from
                        dailydata_transaction_selection
                    where
                        dailydata_transaction_selection."propertyCode" = propertycode 
                        and dailydata_transaction_selection."AsOfDate" = asofdate ;
                            
            
                  DROP TABLE IF EXISTS temp_intermediate2;
                  create temp table temp_intermediate2 as 
                  select
                    "AsOfDate",
                    "Date" ,
                    "Occupancy" ,
                    round("Rate") as "Rate"
                  from
                    snp_dbd_forecast
                  where
                    snp_dbd_forecast."propertyCode" = propertycode 
                    and snp_dbd_forecast."AsOfDate" = asofdate ;



        update temp_high_demand_dates set "Forecast_RMS" = (select temp_intermediate."RoomSold"
                                    
                                  from
                                    temp_intermediate
                                  where
                                    temp_intermediate."Dates"::date = temp_high_demand_dates."StayDate"::date limit 1
                                  )
                  where "StayDate"::date < asofdate;
                  
        update temp_high_demand_dates set "Forecast_RMS" = (select temp_intermediate2."Occupancy" 
                                      
                                    from
                                      temp_intermediate2
                                    where
                                      temp_intermediate2."Date"::date = temp_high_demand_dates."StayDate"::date limit 1
                                  )
                  where "StayDate"::date >= asofdate;

        update temp_high_demand_dates set "Optimal_BAR" = (select temp_intermediate."ADR"
                                    
                                  from
                                    temp_intermediate
                                  where
                                    temp_intermediate."Dates"::date = temp_high_demand_dates."StayDate"::date limit 1
                                  )
                  where "StayDate"::date < asofdate;
                  
        update temp_high_demand_dates set "Optimal_BAR" = (select round(temp_intermediate2."Rate")
                                      
                                    from
                                      temp_intermediate2
                                    where
                                      temp_intermediate2."Date"::date = temp_high_demand_dates."StayDate"::date limit 1
                                  )
                  where "StayDate"::date >= asofdate;


        update temp_high_demand_dates set "R28AVG" = (
                                    select
                                      round(avg("RoomSold"))
                                    from
                                          dailydata_transaction_selection
                                    where
                                      dailydata_transaction_selection."AsOfDate" = asofdate
                                      and dailydata_transaction_selection."Dates" in (
                                                (temp_high_demand_dates."StayDate"::date - interval '7 days') , 
                                                (temp_high_demand_dates."StayDate"::date - interval '14 days') , 
                                                (temp_high_demand_dates."StayDate"::date - interval '21 days') ,
                                                (temp_high_demand_dates."StayDate"::date - interval '28 days')
                                        )
                                  )
                  where "StayDate"::date <= asofdate + interval '6 days';



              DROP TABLE IF EXISTS temp_forcast_r28_after_asofdate_plus_day;   
                  CREATE temp TABLE temp_forcast_r28_after_asofdate_plus_day AS 
                  select
                  asofdate as "AsOfDate",
                    generate_series(
                        asofdate,
                        asofdate + interval '6 days',
                        interval '1 DAY'
                    )::date AS "Dates",0 as "R28AVG";
                    
                    
                    update temp_forcast_r28_after_asofdate_plus_day set "R28AVG" = (
                                    select
                                      round(avg("RoomSold"))
                                    from
                                          dailydata_transaction_selection
                                    where
                                      dailydata_transaction_selection."AsOfDate" = asofdate
                                      and dailydata_transaction_selection."Dates" in (
                                                (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '7 days') , 
                                                (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '14 days') , 
                                                (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '21 days') ,
                                                (temp_forcast_r28_after_asofdate_plus_day."Dates"::date - interval '28 days')
                                        )
                                      
                                  )
                  where "Dates"::date <= asofdate + interval '6 days';
                
                
              -- Remove this entire block or the line referencing the undefined table
          UPDATE temp_high_demand_dates
              SET "R28AVG" = temp_forcast_r28_after_asofdate_plus_day."R28AVG"
              FROM temp_forcast_r28_after_asofdate_plus_day
              WHERE to_char(temp_high_demand_dates."StayDate"::date , 'Day' ) = to_char(temp_forcast_r28_after_asofdate_plus_day."Dates" , 'Day' )
              and temp_high_demand_dates."StayDate"::date > asofdate + interval '6 days';



                DROP TABLE IF EXISTS temp_dailydata_transaction;   
                CREATE temp TABLE temp_dailydata_transaction AS
                  select
                  "OutOfOrder" as "OOO",
                  cast("Dates" as text) as "Date",
                  "Inventory" as "RoomAvailable",
                  cast("AvailableOccupancy" as INTEGER) as "LeftToSell",
                  "RoomSold" as "OnTheBook",
                  cast("Occperc" as INTEGER) as "TotalOCCPercentage",
                  cast("ADR" as INTEGER),
                  cast("TotalRevenue" as INTEGER) as "REV",
                  CAST("RevPAR" as INTEGER),
                  "GroupOTB" AS "GroupOTB",
                            "GroupBlock" AS "GroupBlock"
                from
                  dailydata_transaction_selection dt
                where
                  "AsOfDate" = asofdate
                  and "propertyCode" = propertycode
                  and "Dates" in (select "StayDate"::date from temp_high_demand_dates);

        UPDATE temp_high_demand_dates set "OnTheBook" = (
                                select jsonb_agg(json_build_object(
                                  'OOO', dt."OOO",
                                        'RoomAvailable', dt."RoomAvailable",
                                        'LeftToSell', dt."LeftToSell",
                                        'OnTheBook', dt."OnTheBook",
                                        'TotalOCCPercentage', dt."TotalOCCPercentage",
                                        'ADR', dt."ADR",
                                        'REV', dt."REV",
                                        'RevPAR', dt."RevPAR",
                                        'OTB', dt."GroupOTB",
                                        'Block', dt."GroupBlock"
                                  ))
                                from
                                  temp_dailydata_transaction dt
                                where
                                  temp_high_demand_dates."StayDate"::date = dt."Date"::date
                              );


                DROP TABLE IF EXISTS temp_dailydata_transaction_1Pickup;   
                CREATE temp TABLE temp_dailydata_transaction_1Pickup AS 
                SELECT 
                    sub1."AsOfDate",
                    sub1."Dates",
                    (sub1."OTB1" - sub2."OTB2") AS "RMS",
                    CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2")) as INTEGER ) AS "REV",
                    CASE 
                        WHEN (sub1."OTB1" - sub2."OTB2") <> 0   AND (sub1."TotalRevenue1" - sub2."TotalRevenue2") <> 0 THEN
                            CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2") / (sub1."OTB1" - sub2."OTB2"))   as INTEGER) 
                        ELSE
                            0
                    END AS "ADR"
                FROM (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB1",
                        "TotalRevenue" as "TotalRevenue1" 
                    FROM 
                      dailydata_transaction_selection
                    WHERE
                        "AsOfDate" = asofdate 
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "StayDate"::date from temp_high_demand_dates)
                ) sub1
                LEFT JOIN (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB2",
                        "TotalRevenue" as "TotalRevenue2" 
                    FROM 
                      dailydata_transaction
                    WHERE
                        "AsOfDate" = onedaypickup
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "StayDate"::date from temp_high_demand_dates)
                ) sub2 ON sub1."Dates" = sub2."Dates";

        UPDATE temp_high_demand_dates set "1 Day Pickup" = (
                                select jsonb_agg(json_build_object(
                                  'RMS', dtp."RMS",
                                        'REV', dtp."REV",
                                        'ADR', dtp."ADR"
                                  ))
                                from
                                  temp_dailydata_transaction_1Pickup dtp
                                where
                                  temp_high_demand_dates."StayDate"::date = dtp."Dates"::date
                              );



                DROP TABLE IF EXISTS temp_dailydata_transaction_7Pickup;   
                CREATE temp TABLE temp_dailydata_transaction_7Pickup AS 
                SELECT 
                    sub1."AsOfDate",
                    sub1."Dates",
                    (sub1."OTB1" - sub2."OTB2") AS "RMS",
                    CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2")) as INTEGER ) AS "REV",
                    CASE 
                        WHEN (sub1."OTB1" - sub2."OTB2") <> 0   AND (sub1."TotalRevenue1" - sub2."TotalRevenue2") <> 0 THEN
                            CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2") / (sub1."OTB1" - sub2."OTB2"))   as INTEGER) 
                        ELSE
                            0
                    END AS "ADR"
                FROM (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB1",
                        "TotalRevenue" as "TotalRevenue1" 
                    FROM 
                      dailydata_transaction_selection
                    WHERE
                        "AsOfDate" = asofdate 
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "StayDate"::date from temp_high_demand_dates)
                ) sub1
                LEFT JOIN (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB2",
                        "TotalRevenue" as "TotalRevenue2" 
                    FROM 
                      dailydata_transaction
                    WHERE
                        "AsOfDate" = sevendaypickup
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "StayDate"::date from temp_high_demand_dates)
                ) sub2 ON sub1."Dates" = sub2."Dates";

        UPDATE temp_high_demand_dates set "7 Day Pickup" = (
                                select jsonb_agg(json_build_object(
                                  'RMS', dtp."RMS",
                                        'REV', dtp."REV",
                                        'ADR', dtp."ADR"
                                  ))
                                from
                                  temp_dailydata_transaction_7Pickup dtp
                                where
                                  temp_high_demand_dates."StayDate"::date = dtp."Dates"::date
                              );


              DROP TABLE IF EXISTS temp_rs_history_rate_shop;   
              CREATE temp TABLE temp_rs_history_rate_shop AS        
              select 
                    rp.competiterpropertyname, 
                    trs.* 
                from 
                    rs_history_rate_shop trs 
                left join
                    rev_propertycompetiters rp 
                    on rp.competiterpropertycode = CAST(trs."CompetitorID" as Text) 
                where
                    trs."PropertyCode" = propertycode 
                    and "AsOfDate" = asofdate
                    and "CheckInDate" in (select "StayDate"::date from temp_high_demand_dates)
                    and ("Channel" IS NULL OR "Channel" = channel)
                order by 
                    trs."CheckInDate",
                    "competiterpropertyname";

        UPDATE temp_high_demand_dates set "Rateshop" = (
                                select jsonb_agg(json_build_object(
                                  'competiterpropertyname', trs."competiterpropertyname",
                                  'DayOfWeek', trs."DayOfWeek",
                                  'CompetitorID', trs."CompetitorID",
                                  'Rate', trs."Rate",
                                  'Channel', trs."Channel",
                                  'LOS', trs."LOS",
                                  'RoomType', trs."RoomType",
                                  'IsSelf', trs."IsSelf",
                                  'IsLowestRate', trs."IsLowestRate"
                                  ))
                                from
                                  temp_rs_history_rate_shop trs
                                where
                                  temp_high_demand_dates."StayDate"::date = trs."CheckInDate"::date
                              );
                        
              END $$;	        

              

              select
                *
              from
                temp_high_demand_dates
              order by "StayDate";"""
        highDemandDates_json = fetch_data(conn, highDemandDates_query)

        fdpt = 0
        fourteen_day_pickup_threshold_query = f"""
          select
        distinct CAST(dt."Dates" AS TEXT) as  "Dates",
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '14 day'),0) as "RoomSold" ,
        coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
        coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '14 day'),0) as "Revenue" ,
        case 
          when coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
            coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '14 day'),0) <> 0 
          then round((coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
                coalesce(sum(dt."TotalRevenue") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '14 day'),0))
                /
                (coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
                coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '14 day'),0)))
          else 0
        end as "ADR",
        coalesce(
            CASE
              WHEN '{fdpt}' IS NOT NULL AND '{fdpt}' <> '' AND '{fdpt}' <> 0 THEN '{fdpt}'::INT
              ELSE (
                select
                  (rulevalues::jsonb->0->>'min')::INT
                from
                  rev_propertynotificationconfig rp
                where
                  rp.ruletype = 'Fourteenday Pickup Threshold'
              )
            END,
            0
          ) as threshold
      from
        dailydata_transaction dt
      where
        "AsOfDate" in ('{AS_OF_DATE}','{AS_OF_DATE}'::date - interval '14 day')
        and "Dates" >= '{AS_OF_DATE}'::date - interval '14 day'
        and "propertyCode" = '{PROPERTY_CODE}'
      group by CAST(dt."Dates" AS TEXT)
      having
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'),0) -
        coalesce(sum(dt."RoomSold") filter (where dt."AsOfDate" = '{AS_OF_DATE}'::date - interval '14 day'),0)
        > coalesce(
            CASE
              WHEN '{fdpt}' IS NOT NULL AND '{fdpt}' <> '' AND '{fdpt}' <> 0 THEN '{fdpt}'::INT
              ELSE (
                select
                  (rulevalues::jsonb->0->>'min')::INT
                from
                  rev_propertynotificationconfig rp
                where
                  rp.ruletype = 'Fourteenday Pickup Threshold'
              )
            END,
            0
          )
      order by CAST(dt."Dates" AS TEXT) ;"""
        fourteen_day_pickup_threshold_json = fetch_data(conn, fourteen_day_pickup_threshold_query)

        top_10_event_strategy_query = f"""
        DO $$ 
          declare
                    propertycode text := '{PROPERTY_CODE}';
                    asofdate DATE := '{AS_OF_DATE}';
                    sevendaypickup DATE := '{AS_OF_DATE}'::date - interval '7 day';
                    fourteendaypickup DATE := '{AS_OF_DATE}'::date - interval '14 day';
                    start_date_var DATE := '{AS_OF_DATE}';
                    end_date_var DATE := '{AS_OF_DATE}'::date + interval '2 year';
                    channel text := (select channel_term from rev_rateshopconfig where propertyid = '{PROPERTY_ID}') ;
                begin
  
                DROP TABLE IF EXISTS temp_dates;   
                CREATE temp TABLE temp_dates AS 
                select
                    generate_series(
                        start_date_var,
                        end_date_var,
                        interval '1 DAY'
                    )::date AS "Dates";
                  
                DROP TABLE IF EXISTS temp_rev_all_events;   
                CREATE temp TABLE temp_rev_all_events AS 
                SELECT
                    rae.*
                  FROM
                    rev_events re join rev_all_events rae on re.eventsourceid = rae.event_id
                  WHERE
                      (re.startdate between start_date_var  and end_date_var 
                      or re.enddate between start_date_var  and end_date_var)
					            and re.isactive is true
                  ORDER BY  
                    TO_CHAR(rae.start_date,'yyyy-mm-dd') ,
                    TO_CHAR(rae.end_date,'yyyy-mm-dd') ;

--                DROP TABLE IF EXISTS temp_rev_all_events;   
--                CREATE temp TABLE temp_rev_all_events AS 
--                SELECT
--                    *
--                  FROM
--                    rev_all_events 
--                  WHERE
--                    "propertyCode"=propertycode
--                    and "start_date" between startdate and enddate  
--                  ORDER BY  
--                    TO_CHAR("start_date",'yyyy-mm-dd') ,
--                    TO_CHAR("end_date",'yyyy-mm-dd') ;
                  
                  
                DROP TABLE IF EXISTS temp_selected_dates;   
                CREATE temp TABLE temp_selected_dates as
                select
                  to_char("start_date",'yyyy-mm-dd') as "start_date",
                  to_char("end_date",'yyyy-mm-dd') as "end_date",
                  "title"
                from
                  temp_rev_all_events tr
                left join temp_dates td on tr."start_date" = td."Dates" and tr."end_date" = td."Dates";  
              
              
                DROP TABLE IF EXISTS temp_aggregated_dates;
                  CREATE TEMP TABLE temp_aggregated_dates AS
                  SELECT
                      "start_date" as "Date",
                      STRING_AGG("title", ', ') AS "titles",
                      null as "OnTheBook",
                      null as "7 Day Pickup",
                      null as "14 Day Pickup",
                      null as "Forecast",
                      null as "Rateshop",
                      null as "LastYear",
                      null as "STLY",
                      null as "Event_details"
                  FROM
                      temp_selected_dates
                  GROUP BY
                      "start_date"
                  ORDER BY
                      "start_date";
                    
                  
                  DROP TABLE IF EXISTS temp_dailydata_transaction;   
                CREATE temp TABLE temp_dailydata_transaction AS
                  select
                  "OutOfOrder" as "OOO",
                  cast("Dates" as text) as "Date",
                  "Inventory" as "RoomAvailable",
                  cast("AvailableOccupancy" as INTEGER) as "LeftToSell",
                  "RoomSold" as "OnTheBook",
                  cast("Occperc" as INTEGER) as "TotalOCCPercentage",
                  cast("ADR" as INTEGER),
                  cast("TotalRevenue" as INTEGER) as "REV"
                from
                  dailydata_transaction dt
                where
                  "AsOfDate" = asofdate
                  and "propertyCode" = propertycode
                  and "Dates" in (select "Date"::date from temp_aggregated_dates);
                
                
                
                DROP TABLE IF EXISTS temp_dailydata_transaction_7Pickup;   
                CREATE temp TABLE temp_dailydata_transaction_7Pickup AS 
                SELECT 
                    sub1."AsOfDate",
                    sub1."Dates",
                    (sub1."OTB1" - sub2."OTB2") AS "RMS",
                    CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2")) as INTEGER ) AS "REV",
                    CASE 
                        WHEN (sub1."OTB1" - sub2."OTB2") <> 0   AND (sub1."TotalRevenue1" - sub2."TotalRevenue2") <> 0 THEN
                            CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2") / (sub1."OTB1" - sub2."OTB2"))   as INTEGER) 
                        ELSE
                            0
                    END AS "ADR"
                FROM (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB1",
                        "TotalRevenue" as "TotalRevenue1" 
                    FROM 
                      dailydata_transaction
                    WHERE
                        "AsOfDate" = asofdate 
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "Date"::date from temp_aggregated_dates)
                ) sub1
                LEFT JOIN (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB2",
                        "TotalRevenue" as "TotalRevenue2" 
                    FROM 
                      dailydata_transaction
                    WHERE
                        "AsOfDate" = sevendaypickup
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "Date"::date from temp_aggregated_dates)
                ) sub2 ON sub1."Dates" = sub2."Dates";
              
              
                DROP TABLE IF EXISTS temp_dailydata_transaction_14Pickup;   
                CREATE temp TABLE temp_dailydata_transaction_14Pickup AS 
                SELECT 
                    sub1."AsOfDate",
                    sub1."Dates",
                    (sub1."OTB1" - sub2."OTB2") AS "RMS",
                    CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2")) as INTEGER ) AS "REV",
                    CASE 
                        WHEN (sub1."OTB1" - sub2."OTB2") <> 0   AND (sub1."TotalRevenue1" - sub2."TotalRevenue2") <> 0 THEN
                            CAST(round((sub1."TotalRevenue1" - sub2."TotalRevenue2") / (sub1."OTB1" - sub2."OTB2"))   as INTEGER) 
                        ELSE
                            0
                    END AS "ADR"
                FROM (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB1",
                        "TotalRevenue" as "TotalRevenue1" 
                    FROM 
                      dailydata_transaction
                    WHERE
                        "AsOfDate" = asofdate 
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "Date"::date from temp_aggregated_dates)
                ) sub1
                LEFT JOIN (
                    SELECT 
                        CAST("AsOfDate" as TEXT),
                        CAST("Dates" as TEXT),
                        "RoomSold" as "OTB2",
                        "TotalRevenue" as "TotalRevenue2" 
                    FROM 
                      dailydata_transaction
                    WHERE
                        "AsOfDate" = fourteendaypickup
                        AND "propertyCode" = propertycode
                        AND "Dates" in (select "Date"::date from temp_aggregated_dates)
                ) sub2 ON sub1."Dates" = sub2."Dates";
              
              
              
              DROP TABLE IF EXISTS temp_forecast;
                    create temp table temp_forecast as 
                    select
                        "AsOfDate",
                        "Date",
                        "Occupancy" ,
                        round("Rate") as "Rate"
                    from
                        snp_dbd_forecast
                    where
                        "propertyCode" = propertycode 
                        and "AsOfDate" = asofdate
                        and "Date" in (select "Date"::date from temp_aggregated_dates)
                    ;	
                    
                    
              DROP TABLE IF EXISTS temp_rs_history_rate_shop;   
              CREATE temp TABLE temp_rs_history_rate_shop AS  
              WITH date_range AS (
                SELECT generate_series(
                (select min("Date"::date) from temp_aggregated_dates), 
                (select max("Date"::date) from temp_aggregated_dates), 
                INTERVAL '1 day'
              )::date AS "CheckInDate"),
              competitors AS (
                  SELECT 
                      CAST(competiterpropertycode AS INTEGER) AS competiterpropertycode,
                      competiterpropertyname,
                      isself
                  FROM rev_propertycompetiters
              )
              SELECT 
                  c.competiterpropertyname,
                  c.competiterpropertycode as "CompetitorID",
                  c.isself as "IsSelf",
                  CAST(d."CheckInDate" AS TEXT) as "CheckInDate",
                  to_char(d."CheckInDate",'Day') as "DayOfWeek",
                  COALESCE(trs."Rate", 0) AS "Rate",
				  COALESCE(trs."LOS", 0) AS "LOS",
                  COALESCE(trs."RoomType", '') AS "RoomType",
                  COALESCE(trs."Channel", channel) AS "Channel",
                  COALESCE(CAST(trs."IsLowestRate" AS BOOLEAN), true) AS "IsLowestRate"
              FROM 
                  competitors c
              CROSS JOIN 
                  date_range d
              LEFT JOIN 
                  rs_history_rate_shop trs 
                  ON c.competiterpropertycode = trs."CompetitorID"
                  AND trs."CheckInDate" = d."CheckInDate"
                  AND trs."AsOfDate" = (select MAX("AsOfDate" ) from rs_history_rate_shop rhrs)
                  AND trs."Channel" = channel
                  AND trs."PropertyCode" = propertycode
              ORDER BY 
                  d."CheckInDate",
                  c.competiterpropertyname;

                  
              DROP TABLE IF EXISTS temp_dailydata_transaction_LastYear;   
              CREATE temp TABLE temp_dailydata_transaction_LastYear AS       
              select
                    CAST("Dates" as TEXT) AS "Dates",
                    "RoomSold" AS "RMS",
                    round(CAST("TotalRevenue" as INTEGER)) AS "REV",
                    case 
                      when "RoomSold" <> 0 
                        then round(("TotalRevenue"/"RoomSold"))
                      else 0 
                    end as "ADR"
                from
                  dailydata_transaction dt
                where
                  "AsOfDate" = asofdate
                  and "propertyCode" = propertycode
                  and "Dates" in (select "Date"::date - interval '1 year' from temp_aggregated_dates)
                order by "Dates";
                
              
              DROP TABLE IF EXISTS temp_dailydata_transaction_STLY;   
              CREATE temp TABLE temp_dailydata_transaction_STLY AS
              select
                  cast("StayDate" as text) as "Dates",
                  sum("RoomNight") as "RMS",
                  round(sum("Rate")) as "REV",
                  case 
                  when sum("RoomNight") <> 0 
                    then round((sum("Rate")/sum("RoomNight")))
                  else 0 
                  end as "ADR" 
                from
                  copy_mst_reservation
                where
                  "AsOfDate" = asofdate
                  and "propertyCode" = propertycode
                  and "Status" in ('R', 'O', 'I')
                  and "StayDate" in (select "Date"::date - interval '1 year' from temp_aggregated_dates)
                  and "BookingDate" <= (asofdate::date - interval '1 year')
                group by
                  "StayDate"
                order by
                  "StayDate";
          
        UPDATE temp_aggregated_dates set "OnTheBook" = (
                                select jsonb_agg(json_build_object(
                                  'OOO', dt."OOO",
                                        'RoomAvailable', dt."RoomAvailable",
                                        'LeftToSell', dt."LeftToSell",
                                        'OnTheBook', dt."OnTheBook",
                                        'TotalOCCPercentage', dt."TotalOCCPercentage",
                                        'ADR', dt."ADR",
                                        'REV', dt."REV"
                                  ))
                                from
                                  temp_dailydata_transaction dt
                                where
                                  temp_aggregated_dates."Date"::date = dt."Date"::date
                              );
                            
                            
        UPDATE temp_aggregated_dates set "7 Day Pickup" = (
                                select jsonb_agg(json_build_object(
                                  'RMS', dtp."RMS",
                                        'REV', dtp."REV",
                                        'ADR', dtp."ADR"
                                  ))
                                from
                                  temp_dailydata_transaction_7Pickup dtp
                                where
                                  temp_aggregated_dates."Date"::date = dtp."Dates"::date
                              );
                            
                            
        UPDATE temp_aggregated_dates set "14 Day Pickup" = (
                                select jsonb_agg(json_build_object(
                                  'RMS', dtp."RMS",
                                        'REV', dtp."REV",
                                        'ADR', dtp."ADR"
                                  ))
                                from
                                  temp_dailydata_transaction_14Pickup dtp
                                where
                                  temp_aggregated_dates."Date"::date = dtp."Dates"::date
                              ); 	
                            
                            
        UPDATE temp_aggregated_dates set "Forecast" = (
                                select jsonb_agg(json_build_object(
                                  'RMS', tf."Occupancy"
                                  ))
                                from
                                  temp_forecast tf
                                where
                                  temp_aggregated_dates."Date"::date = tf."Date"::date
                              ); 
                            
                            
        UPDATE temp_aggregated_dates set "Rateshop" = (
                                select jsonb_agg(json_build_object(
                                  'competiterpropertyname', trs."competiterpropertyname",
                                  'DayOfWeek', trs."DayOfWeek",
                                  'CompetitorID', trs."CompetitorID",
                                  'Rate', trs."Rate",
                                  'Channel', trs."Channel",
                                  'LOS', trs."LOS",
                                  'RoomType', trs."RoomType",
                                  'IsSelf', trs."IsSelf",
                                  'IsLowestRate', trs."IsLowestRate"
                                  ))
                                from
                                  temp_rs_history_rate_shop trs
                                where
                                  temp_aggregated_dates."Date"::date = trs."CheckInDate"::date
                              );
                            
                            
        UPDATE temp_aggregated_dates set "LastYear" = (
                                select jsonb_agg(json_build_object(
                                  'RMS', tdly."RMS",
                                  'REV', tdly."REV",
                                  'ADR', tdly."ADR"
                                  ))
                                from
                                  temp_dailydata_transaction_LastYear tdly
                                where
                                  temp_aggregated_dates."Date"::date = tdly."Dates"::date + interval '1 year'
                              );
                            
                            
        UPDATE temp_aggregated_dates set "STLY" = (
                                select jsonb_agg(json_build_object(
                                  'RMS', tdstly."RMS",
                                  'REV', tdstly."REV",
                                  'ADR', tdstly."ADR"
                                  ))
                                from
                                  temp_dailydata_transaction_STLY tdstly
                                where
                                  temp_aggregated_dates."Date"::date = tdstly."Dates"::date + interval '1 year'
                              ); 	
                              
                              
        UPDATE temp_aggregated_dates set "Event_details" = (
                                select jsonb_agg(json_build_object(
                                  'pullDateId', tr."pullDateId",
                                  'AsOfDate', tr."AsOfDate",
                                  'event_id', tr."event_id",
                                  'title', tr."title",
                                  'description', tr."description",
                                  'category', tr."category",
                                  'labels', tr."labels",
                                  'rank', tr."rank",
                                  'local_rank', tr."local_rank",
                                  'phq_attendance', tr."phq_attendance",
                                  'type', tr."type",
                                  'start_date', tr."start_date",
                                  'end_date', tr."end_date",
                                  'timezone', tr."timezone",
                                  'geo_lat', tr."geo_lat",
                                  'geo_long', tr."geo_long",
                                  'place_hierarchies', tr."place_hierarchies",
                                  'state', tr."state",
                                  'private', tr."private"
                                  ))
                                from
                                  temp_rev_all_events tr
                                where
                                  temp_aggregated_dates."Date"::date = tr."start_date"::date
                              );
                    
                  
        END $$;	        
  
        
  
        select
          *
        from
          temp_aggregated_dates
        order by "Date" limit 10;"""
        top_10_event_strategy_json = fetch_data(conn, top_10_event_strategy_query)

        response_json = {
            "daily_performance_dashboard": daily_performance_dashboard_json,
            "adr_by_bookingdate" : adr_by_bookingdate_json,
            "top10marketsegment_drilldown" : top10marketsegment_drilldown_json,
            "dashboard_revglance" : dashboard_revglance_json,
            "marketsegment_mix_chart" : marketsegment_mix_chart_json,
            "one_day_pickup_threshold" : one_day_pickup_threshold_json,
            "candle_chart" : candle_chart_json,
            "comp_rate_variance_with_occ_self" : comp_rate_variance_with_occ_self_json,
            "lowDemandDates" : lowDemandDates_json,
            "forecast_mix_chart" : forecast_mix_chart_json,
            "booking_pace_comparison_chart" : booking_pace_comparison_chart_json,
            "seven_day_pickup_threshold" : seven_day_pickup_threshold_json,
            "highDemandDates" : highDemandDates_json,
            "fourteen_day_pickup_threshold" : fourteen_day_pickup_threshold_json,
            "top_10_event_strategy" : top_10_event_strategy_json,
        }
        
        return response_json, error_list
    except Exception as e:
        err_msg = f"Error fetching Performance Monitor data: {str(e)}"
        traceback_info = traceback.format_exc()
        error_list.append(f"{err_msg}\nTraceback:\n{traceback_info}")
        return None, error_list

def get_AnnualSummary(PROPERTY_ID, PROPERTY_CODE, AS_OF_DATE, CLIENT_ID, year, conn, componentname):
        try:
            error_list = []
            response_json = None
            total_ly_query = """
                SELECT 
                    "propertyCode",
                    "AsOfDate"::text AS "AsOfDate", 
                    "year",
                    "month",
                    "occ",
                    "rms",
                    "adr",
                    "rev"
                FROM snp_annsmry_total_ly 
                WHERE "AsOfDate" = :as_of_date
                AND "propertyCode" = :property_code;
                                        """
            print(total_ly_query)

            params = {"as_of_date": AS_OF_DATE, "property_code": PROPERTY_CODE}
            if year:
                total_ly_query += " AND \"year\" = :year"
                params["year"] = year

            total_ly_json = fetch_data(conn, total_ly_query, {
                "as_of_date": AS_OF_DATE,
                "property_code": PROPERTY_CODE,
                "year": year
            })
            print(total_ly_json)

            otb_query = """
                SELECT 
                    "propertyCode",
                    "AsOfDate"::text AS "AsOfDate",
                    "year",
                    "month",
                    "occ",
                    "rms",
                    "adr",
                    "rev"
                FROM snp_annsmry_on_the_book
                WHERE "AsOfDate" = :as_of_date
                AND "propertyCode" = :property_code
                AND "year" = :year;
            """

            otb_json = fetch_data(conn, otb_query, {
                "as_of_date": AS_OF_DATE,
                "property_code": PROPERTY_CODE,
                "year": year
            })

            net_stly_query = """
                SELECT 
                    "propertyCode",
                    "AsOfDate"::text AS "AsOfDate",
                    "year",
                    "month",
                    "occ",
                    "rms",
                    "adr",
                    "rev"
                FROM snp_annsmry_net_stly
                WHERE "AsOfDate" = :as_of_date
                AND "propertyCode" = :property_code
                AND "year" = :year;
            """

            net_stly_json = fetch_data(conn, net_stly_query, {
                "as_of_date": AS_OF_DATE,
                "property_code": PROPERTY_CODE,
                "year": year
            })

            response_json = {
                "otb_current": otb_json,
                "net_stly": net_stly_json,
                "total_ly": total_ly_json
            }
            print(response_json)
            
            return response_json, error_list

        except Exception as e:
            err_msg = f"Error fetching {componentname} data: {str(e)}"
            error_list.append(err_msg)
            return None, error_list 
 