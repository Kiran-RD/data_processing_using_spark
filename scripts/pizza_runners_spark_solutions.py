# %%
from pyspark.sql import SparkSession
spark_hive = SparkSession.builder\
                        .appName('Spark Solutions')\
                        .master('local[*]')\
                        .config('hive.metastore_uris','thrift://localhost:9083/')\
                        .config('spark.sql_warehouse.dir','hdfs://localhost:9000/user/hive/warehouse/')\
                        .enableHiveSupport().getOrCreate()
# %%
# runnersDF = spark_hive.sql('select * from pizza_runners.runners;')
# pizza_namesDF = spark_hive.sql('select * from pizza_runners.pizza_names;')
# customer_ordersDF = spark_hive.sql('select * from pizza_runners.customer_orders;')
# runner_ordersDF = spark_hive.sql('select * from pizza_runners.runner_orders;')
# pizza_toppingsDF = spark_hive.sql('select * from pizza_runners.pizza_toppings;')
# pizza_recipesDF = spark_hive.sql('select * from pizza_runners.pizza_recipes;')
# # %%
# runnersDF.show(truncate=False)
# pizza_namesDF.show(truncate=False)
# customer_ordersDF.show(truncate=False)
# runner_ordersDF.show(truncate=False)
# pizza_toppingsDF.show(truncate=False)
# pizza_recipesDF.show(truncate=False)
# %%
""" 1. How many pizzas were ordered? """
Q1_res = spark_hive.sql("select 'Total_Orders' desc, count(*) count from pizza_runners.runner_orders")
Q1_res.write.mode('overwrite').saveAsTable('pizza_runners.q1_res')
# %%
""" 2. How many unique customer orders were made? """
Q2_res = spark_hive.sql("select 'Unique_Customer_Orders' desc, count(*) count from (select distinct customer_id from pizza_runners.customer_orders)")
Q2_res.write.mode('overwrite').saveAsTable('pizza_runners.q2_res')

# %%
""" 3. How many successful orders were delivered by each runner? """
Q3_res = spark_hive.sql("select runner_id, count(*) successful_deliveries from pizza_runners.runner_orders where cancellation not like '%Cancellation' group by runner_id")
Q3_res.write.mode('overwrite').saveAsTable('pizza_runners.q3_res')

# %%
""" 4. How many of each type of pizza was delivered? """
Q4_res = spark_hive.sql("\
               select pizza_id, count(*) count_of_deliveries\
               from pizza_runners.customer_orders custo\
               join pizza_runners.runner_orders runo on custo.order_id = runo.order_id \
               where runo.cancellation not like '%Cancellation'\
               group by custo.pizza_id\
               ")
Q4_res.write.mode('overwrite').saveAsTable('pizza_runners.q4_res')

# %%
""" 5. How many Vegetarian and Meatlovers were ordered by each customer? """
Q5_res = spark_hive.sql("\
                select custo.customer_id, pn.pizza_name, count(*) count_of_orders\
                from pizza_runners.customer_orders custo\
                join pizza_runners.pizza_names pn on custo.pizza_id = pn.pizza_id\
                group by custo.customer_id, pn.pizza_name\
               ")
Q5_res.write.mode('overwrite').saveAsTable('pizza_runners.q5_res')

# %%
""" 6. What was the maximum number of pizzas delivered in a single order? """
q6_res = spark_hive.sql("\
                select 'Max_Pizzas_Per_Order' desc, max(pizzas_per_order) count from (\
               select custo.order_id, count(*) pizzas_per_order\
               from pizza_runners.customer_orders custo\
               join pizza_runners.runner_orders runo on custo.order_id = runo.order_id \
               where runo.cancellation not like '%Cancellation'\
               group by custo.order_id)\
               ")
q6_res.write.mode('overwrite').saveAsTable('pizza_runners.q6_res')

# %%
""" 7. For each customer, how many delivered pizzas had at least 1 change and how many had no changes? """
Q7_res = spark_hive.sql("\
                select 'At least one change' desc, count(*) count from pizza_runners.customer_orders where exclusions != '' or extras != ''\
                union\
                select 'NO Changes' desc, count(*) count from pizza_runners.customer_orders where exclusions = '' and extras = ''\
               ")
Q7_res.write.mode('overwrite').saveAsTable('pizza_runners.q7_res')

# %%
""" 8. How many pizzas were delivered that had both exclusions and extras? """
Q8_res = spark_hive.sql("\
                select 'Exclusions and Extras' desc, count(*) count from pizza_runners.customer_orders where exclusions != '' and extras != ''\
               ")
Q8_res.write.mode('overwrite').saveAsTable('pizza_runners.q8_res')

# %%
""" 9. What was the total volume of pizzas ordered for each hour of the day? """
Q9_res= spark_hive.sql("\
                select hour_of_day, count(*) count_of_orders from (\
                select date_format(order_time, 'hh') hour_of_day from pizza_runners.customer_orders)\
                group by hour_of_day\
               ")
Q9_res.write.mode('overwrite').saveAsTable('pizza_runners.q9_res')

# %%
""" 10. What was the volume of orders for each day of the week? """
Q10_res = spark_hive.sql("\
                select day_of_week, count(*) count_of_orders from (\
                select dayofweek(order_time) day_of_week from pizza_runners.customer_orders)\
                group by day_of_week\
               ")
Q10_res.write.mode('overwrite').saveAsTable('pizza_runners.q10_res')
# # %%
# Q1_res.show()
# Q2_res.show()
# Q3_res.show()
# Q4_res.show()
# Q5_res.show()
# Q6_res.show()
# Q7_res.show()
# Q8_res.show()
# Q9_res.show()
# Q10_res.show()