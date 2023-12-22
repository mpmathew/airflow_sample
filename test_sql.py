create_forestfire_table = ["""USE TEST_DEV_DB.TEST_SCHEMA;""",
                           """
    CREATE OR REPLACE TRANSIENT TABLE {{ params.table_name }}
        (
            id INT,
            y INT,
            month VARCHAR(25),
            day VARCHAR(25),
            ffmc FLOAT,
            dmc FLOAT,
            dc FLOAT,
            isi FLOAT,
            temp FLOAT,
            rh FLOAT,
            wind FLOAT,
            rain FLOAT,
            area FLOAT
        );
"""]

create_cost_table = """
    CREATE OR REPLACE TRANSIENT TABLE {{ params.table_name }}
        (
            id INT,
            land_damage_cost INT,
            property_damage_cost INT,
            lost_profits_cost INT
        );
"""

create_forestfire_cost_table = """
    CREATE OR REPLACE TRANSIENT TABLE {{ params.table_name }}
        (
            id INT,
            land_damage_cost INT,
            property_damage_cost INT,
            lost_profits_cost INT,
            total_cost INT,
            y INT,
            month VARCHAR(25),
            day VARCHAR(25),
            area FLOAT
        );
"""

load_forestfire_data = """
    INSERT INTO {{ params.table_name }} VALUES
        (1,2,'aug','fri',91,166.9,752.6,7.1,25.9,41,3.6,0,100),
        (2,2,'feb','mon',84,9.3,34,2.1,13.9,40,5.4,0,57.8),
        (3,4,'mar','sat',69,2.4,15.5,0.7,17.4,24,5.4,0,92.9),
        (4,4,'mar','mon',87.2,23.9,64.7,4.1,11.8,35,1.8,0,1300),
        (5,5,'mar','sat',91.7,35.8,80.8,7.8,15.1,27,5.4,0,4857),
        (6,5,'sep','wed',92.9,133.3,699.6,9.2,26.4,21,4.5,0,9800),
        (7,5,'mar','fri',86.2,26.2,94.3,5.1,8.2,51,6.7,0,14),
        (8,6,'mar','fri',91.7,33.3,77.5,9,8.3,97,4,0.2,74.5),
        (9,9,'feb','thu',84.2,6.8,26.6,7.7,6.7,79,3.1,0,8880.7);
"""

load_cost_data = """
    INSERT INTO {{ params.table_name }} VALUES
        (1,150000,32000,10000),
        (2,200000,50000,50000),
        (3,90000,120000,300000),
        (4,230000,14000,7000),
        (5,98000,27000,48000),
        (6,72000,800000,0),
        (7,50000,2500000,0),
        (8,8000000,33000000,0),
        (9,6325000,450000,76000);
"""

load_forestfire_cost_data = """
    INSERT INTO forestfire_costs (
            id, land_damage_cost, property_damage_cost, lost_profits_cost,
            total_cost, y, month, day, area
        )
        SELECT
            c.id,
            c.land_damage_cost,
            c.property_damage_cost,
            c.lost_profits_cost,
            c.land_damage_cost + c.property_damage_cost + c.lost_profits_cost,
            ff.y,
            ff.month,
            ff.day,
            ff.area
        FROM costs c
        LEFT JOIN forestfires ff
            ON c.id = ff.id
"""

transform_forestfire_cost_table = """
    SELECT
        id,
        month,
        day,
        total_cost,
        area,
        total_cost / area as cost_per_area
    FROM {{ params.table_name }}
"""
