create database if not exists pizza_runners;
use pizza_runners;

drop table if exists customer_orders;
drop table if exists runners;
drop table if exists pizza_names;
drop table if exists runner_orders;
drop table if exists pizza_toppings;
drop table if exists pizza_recipes;

-- select * from pizza_runners.runners;
-- select * from pizza_runners.pizza_names;
-- select * from pizza_runners.customer_orders;
-- select * from pizza_runners.runner_orders;
-- select * from pizza_runners.pizza_toppings;
-- select * from pizza_runners.pizza_recipes;

create table if not exists runners(
    runner_id int primary key,
    registration_date date
);

create table if not exists runner_orders(
    order_id int primary key,
    runner_id int,
    pickup_time TIMESTAMP,
    distance int,
    duration int,
    cancellation varchar(100)
);

create table if not exists pizza_names(
    pizza_id int primary key,
    pizza_name varchar(100)
);

create table if not exists pizza_recipes(
    pizza_id int primary key,
    toppings varchar(100)
);


create table if not exists pizza_toppings(
    topping_id int primary key,
    topping_name varchar(100)
);

create table if not exists customer_orders(
    order_id int,
    customer_id int,
    pizza_id int,
    exclusions varchar(100),
    extras varchar(100),
    order_time timestamp,
    primary key(order_id, customer_id, pizza_id),
    foreign key (order_id) references runner_orders(order_id),
    foreign key (pizza_id) references pizza_names(pizza_id)
);

SET GLOBAL local_infile = true;

load data local infile '/home/saif/LFS/cohort_c8/Usecase_2/data/runners.csv'
into table runners
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

load data local infile '/home/saif/LFS/cohort_c8/Usecase_2/data/pizza_names.csv'
into table pizza_names
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

load data local infile '/home/saif/LFS/cohort_c8/Usecase_2/data/pizza_recipes.csv'
into table pizza_recipes
fields terminated by '|'
lines terminated by '\n'
ignore 1 rows;

load data local infile '/home/saif/LFS/cohort_c8/Usecase_2/data/pizza_toppings.csv'
into table pizza_toppings
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

load data local infile '/home/saif/LFS/cohort_c8/Usecase_2/data/runner_orders.csv'
into table runner_orders
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 rows
(order_id,runner_id,@pickup_time,@distance,@duration,@cancellation)
set pickup_time = case @pickup_time
                    when 'null' then NULL
                    else @pickup_time
                    end,
    distance = case @distance
                    when 'null' then NULL
                    else @distance+0
                    end,
    duration = case @duration
                    when 'null' then NULL
                    else @duration+0
                    end,
    cancellation = case @cancellation
                    when 'null' then NULL
                    when 'NaN' then NULL
                    when '' then NULL
                    else @cancellation                    
                    end
;

load data local infile '/home/saif/LFS/cohort_c8/Usecase_2/data/customer_orders.csv'
into table customer_orders
fields terminated by ','
optionally enclosed by '"'
lines terminated by '\n'
ignore 1 rows
(order_id,customer_id,pizza_id,@exclusions,@extras,order_time)
set exclusions = case @exclusions
                    when 'null' then ''
                    else @exclusions
                    end,
    extras = case @extras
                when 'NaN' then ''
                when 'null' then ''
                else @extras
                end
;