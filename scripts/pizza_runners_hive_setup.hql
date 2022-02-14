create database if not exists pizza_runners;

drop table if exists pizza_runners.runners;
drop table if exists pizza_runners.pizza_names;
drop table if exists pizza_runners.customer_orders;
drop table if exists pizza_runners.runner_orders;
drop table if exists pizza_runners.pizza_toppings;
drop table if exists pizza_runners.pizza_recipes;

-- select * from pizza_runners.runners;
-- select * from pizza_runners.pizza_names;
-- select * from pizza_runners.customer_orders;
-- select * from pizza_runners.runner_orders;
-- select * from pizza_runners.pizza_toppings;
-- select * from pizza_runners.pizza_recipes;

create table if not exists pizza_runners.runners(
    runner_id int,
    registration_date date
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n";

create table if not exists pizza_runners.runner_orders(
    order_id int,
    runner_id int,
    pickup_time timestamp,
    distance int,
    duration int,
    cancellation string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n";

create table if not exists pizza_runners.pizza_names(
    pizza_id int,
    pizza_name string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n";

create table if not exists pizza_runners.customer_orders(
    order_id int,
    customer_id int,
    pizza_id int,
    exclusions string,
    extras string,
    order_time timestamp
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n";


create table if not exists pizza_runners.pizza_recipes(
    pizza_id int,
    toppings string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n";


create table if not exists pizza_runners.pizza_toppings(
    topping_id int,
    topping_name string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ","
LINES TERMINATED BY "\n";

drop table if exists pizza_runners.q1_res;
drop table if exists pizza_runners.q2_res;
drop table if exists pizza_runners.q3_res;
drop table if exists pizza_runners.q4_res;
drop table if exists pizza_runners.q5_res;
drop table if exists pizza_runners.q6_res;
drop table if exists pizza_runners.q7_res;
drop table if exists pizza_runners.q8_res;
drop table if exists pizza_runners.q9_res;
drop table if exists pizza_runners.q10_res;

create table if not exists pizza_runners.q1_res(
    desc string,
    count int
);

create table if not exists pizza_runners.q2_res(
    desc string,
    count int
);

create table if not exists pizza_runners.q3_res(
    runner_id int,
    successful_deliveries int
);

create table if not exists pizza_runners.q4_res(
    pizza_id int,
    count_of_deliveries int
);

create table if not exists pizza_runners.q5_res(
    customer_id int,
    pizza_name string,
    count_of_orders int
);

create table if not exists pizza_runners.q6_res(
    desc string,
    count int
);

create table if not exists pizza_runners.q7_res(
    desc string,
    count int
);

create table if not exists pizza_runners.q8_res(
    desc string,
    count int
);

create table if not exists pizza_runners.q9_res(
    hour_of_day int,
    count_of_orders int
);

create table if not exists pizza_runners.q10_res(
    day_of_week int,
    count_of_orders int
);